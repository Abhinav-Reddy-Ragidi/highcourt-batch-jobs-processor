import boto3
import io
import os
import pdfplumber#type:ignore
import nltk#type:ignore
import asyncio
import concurrent.futures
import json
import openai
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from collections import defaultdict
from datetime import datetime, date
import pickle
from nltk.tokenize import sent_tokenize#type:ignore
import tiktoken
from openai import OpenAI
import time
import uuid

# Download punkt tokenizer once
nltk.download("punkt")

# Constants / Configs
AWS_REGION = 'ap-south-1'
AWS_REGION_S3_VECTORS = "ap-southeast-2"  # Your region here
DYNAMODB_TABLE = "high-court-judgments"
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
# openai.api_key = OPENAI_API_KEY
client = OpenAI(api_key= OPENAI_API_KEY)

# Clients
s3 = boto3.client('s3')
s3_vectors = boto3.client("s3vectors", region_name=AWS_REGION_S3_VECTORS)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE)

# Embedding model config
EMBED_MODEL = "text-embedding-3-small"
EMBED_DIM = 1536
CHUNK_SIZE = 2000
CHUNK_OVERLAP = 100

encoding = tiktoken.encoding_for_model("text-embedding-3-small")

# Generate unique process ID
PROCESS_ID = f"proc_{uuid.uuid4().hex[:8]}_{int(time.time())}"

# Helper: Approximate token count by splitting words (very rough)
def count_tokens(text):
    return len(encoding.encode(text))

# Split text into chunks <= max_tokens, respecting sentence boundaries
def chunk_text(text, max_tokens=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    sentences = sent_tokenize(text)
    chunks = []
    current_chunk = []
    current_len = 0

    for sent in sentences:
        sent_len = count_tokens(sent)

        if current_len + sent_len > max_tokens:
            # Save current chunk
            chunks.append(" ".join(current_chunk))

            # Start next chunk with overlap from end of last chunk
            overlap_tokens = []
            token_count = 0
            for prev_sent in reversed(current_chunk):
                prev_len = count_tokens(prev_sent)
                if token_count + prev_len <= overlap:
                    overlap_tokens.insert(0, prev_sent)
                    token_count += prev_len
                else:
                    break

            current_chunk = overlap_tokens + [sent]
            current_len = sum(count_tokens(s) for s in current_chunk)
        else:
            current_chunk.append(sent)
            current_len += sent_len

    if current_chunk:
        chunks.append(" ".join(current_chunk))

    return chunks

# Extract text from PDF bytes
def extract_text_from_pdf(pdf_bytes):
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        text = ""
        for page in pdf.pages:
            text += page.extract_text() or ""
    return text

# Load parquet metadata from bytes into list of dicts
def load_metadata_from_parquet(parquet_bytes):
    table = pq.read_table(source=io.BytesIO(parquet_bytes))
    # Convert to list of dicts
    records = table.to_pylist()
    return records

# Generate embedding using OpenAI
def generate_embedding(text):
    try:
        response = client.embeddings.create(
            input=text,
            model=EMBED_MODEL
        )
        return response.data[0].embedding
    except Exception as e:
        print(f"Error generating embedding: {e}")
        return None

def save_titles_and_stats_separately(court_name, year, successful_titles, processing_stats):
    """
    Save titles and statistics separately for each processing run.
    
    Structure:
    - Titles: {court_name}/titles-{year}-{process_id}.pkl
    - Stats: {court_name}/stats-{year}-{process_id}.json
    
    :param court_name: str - Name of the court
    :param year: str - Year of processing  
    :param successful_titles: list - List of successfully processed titles
    :param processing_stats: dict - Statistics for this processing run
    """
    bucket = "all-title-files"
    court_name = court_name.replace('--', '-').replace(' ', '-')
    
    # File paths
    titles_key = f"{court_name}/titles/{year}.pkl"
    stats_key = f"{court_name}/stats/{year}.json"
    
    try:
        # Save titles as pickle file
        titles_data = successful_titles
        titles_pkl_bytes = pickle.dumps(titles_data)
        s3.put_object(Bucket=bucket, Key=titles_key, Body=titles_pkl_bytes)
        
        # Save statistics as JSON file
        stats_json = json.dumps(processing_stats, indent=2, default=str)
        s3.put_object(Bucket=bucket, Key=stats_key, Body=stats_json.encode('utf-8'))
        
        print(f"Saved files for {court_name} year {year}:")
        print(f"  Titles: {titles_key} ({len(successful_titles)} titles)")
        print(f"  Stats: {stats_key}")
        
        return titles_key, stats_key
        
    except Exception as e:
        print(f"Error saving files for {court_name} year {year}: {e}")
        return None, None

# Upload JSON chunk embedding file to vector bucket
def upload_chunk_to_s3_vector(bucket, index_name, chunk_id, embedding, metadata):
    # Merge text into metadata for direct retrieval
    index_name = index_name.replace('--', '-')
    cnr = metadata.get('cnr')

    # Create key in required format
    vector_key = f"{cnr}_chunk_{chunk_id}"

    # Add vector to S3 Vector index
    s3_vectors.put_vectors(
        vectorBucketName=bucket,
        indexName=index_name.lower(),
        vectors=[
            {
                "key": vector_key,
                "data": {"float32": embedding},
                "metadata": metadata
            }
        ]
    )

    print(f"[Vector Index] Added {vector_key} to index '{index_name}'")

# Put metadata record in DynamoDB
def put_metadata_to_dynamodb(record):
    pk = record.get("cnr")
    if not pk:
        print("Skipping record without CNR")
        return False

    try:
        record.pop("raw_html", None)
        # Convert date fields in-place to ISO strings
        for field in ["date_of_registration", "decision_date"]:
            if field in record and isinstance(record[field], (datetime, date)):
                record[field] = record[field].isoformat()

        # Start with your PK/SK
        item = {
            "pk": pk,
            "sk": "HC_DOC_METADATA"
        }

        # Add all other fields from record
        for key, value in record.items():
            if key.lower() != "cnr":  # Avoid overwriting PK
                item[key] = value

        table.put_item(Item=item)
        return True

    except Exception as e:
        print(f"Error putting item to DynamoDB for CNR {pk}: {e}")
        return False

# Main processing function for one PDF file + metadata record
def process_file(source_bucket, vector_bucket, target_bucket, metadata_record, pdf_key, court_name, processing_stats, year):
    """Process a single file and update statistics"""
    filename = os.path.basename(pdf_key)
    print(f"Processing {pdf_key}...")
    
    doc_start_time = time.time()
    processing_stats['total_documents_attempted'] += 1
    
    try:
        # 1. Download PDF
        pdf_start_time = time.time()
        try:
            pdf_obj = s3.get_object(Bucket=target_bucket, Key=pdf_key)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                print(f"Key not found: {pdf_key} in bucket {target_bucket}")
                processing_stats['keys_not_found'] += 1
                return
            else:
                processing_stats['failed_files'].append({
                    'filename': filename,
                    'error': 'error_with_getting_file',
                    'error_message': f"Key not found: {pdf_key}"
                })
                processing_stats['failed_documents'] += 1
                raise
        
        pdf_bytes = pdf_obj["Body"].read()
        pdf_extraction_time = time.time() - pdf_start_time
        
        # 2. Extract text
        try:
            text = extract_text_from_pdf(pdf_bytes)
        except Exception as e:
            processing_stats['pdf_extraction_errors'] += 1
            processing_stats['failed_files'].append({
                'filename': filename,
                'error': 'pdf_extraction_error', 
                'error_message': str(e)
            })
            processing_stats['failed_documents'] += 1
            return
        
        if not text.strip():
            print(f"No extractable text found in {pdf_key}, skipping embedding.")
            processing_stats['empty_text_documents'] += 1
            processing_stats['failed_files'].append({
                'filename': filename,
                'error': 'empty_text',
                'error_message': 'No extractable text found in PDF'
            })
            processing_stats['failed_documents'] += 1
            return

        # 3. Count tokens and chunk text
        token_count = count_tokens(text)
        chunks = chunk_text(text, max_tokens=CHUNK_SIZE)
        chunk_count = len(chunks)
        
        # 4. Generate embeddings and upload chunks
        embedding_start_time = time.time()
        successful_embeddings = 0
        
        for idx, chunk in enumerate(chunks):
            embedding = generate_embedding(chunk)
            
            if embedding is None:
                processing_stats['embedding_generation_errors'] += 1
                continue
                
            try:
                # Upload chunk embedding JSON to vector bucket
                metadata = {
                    "cnr": metadata_record.get('cnr'),
                    "content": chunk,
                    "title": metadata_record.get('title'),
                    "year": year,
                }
                upload_chunk_to_s3_vector(vector_bucket, court_name, idx, embedding, metadata)
                successful_embeddings += 1
            except Exception as e:
                processing_stats['s3_upload_errors'] += 1
                print(f"Error uploading chunk {idx} for {filename}: {e}")

        embedding_total_time = time.time() - embedding_start_time
        
        # 5. Put metadata + S3 key info into DynamoDB
        metadata_record["s3_key"] = pdf_key
        dynamodb_success = put_metadata_to_dynamodb(metadata_record)
        
        if not dynamodb_success:
            processing_stats['dynamodb_write_errors'] += 1
        
        # 6. Record successful processing
        doc_processing_time = time.time() - doc_start_time
        title = metadata_record.get('title', filename)
        
        processing_stats['successful_documents'] += 1
        processing_stats['successful_titles'].append(title)
        processing_stats['token_counts'].append(token_count)
        processing_stats['chunk_counts'].append(chunk_count)
        # processing_stats['processing_times'].append(doc_processing_time)
        # processing_stats['pdf_extraction_times'].append(pdf_extraction_time)
        # processing_stats['embedding_generation_times'].append(embedding_total_time)
        
        print(f"✓ Successfully processed {filename}: {token_count} tokens, {chunk_count} chunks, {successful_embeddings} embeddings")
        
    except Exception as e:
        processing_stats['failed_documents'] += 1
        processing_stats['failed_files'].append({
            'filename': filename,
            'error': 'general_error',
            'error_message': str(e)
        })
        print(f"✗ Error processing {filename}: {e}")

# Main async runner for all files in prefix
async def main_async(source_bucket, vector_bucket, target_bucket, year, court_code, bench):
    # Build prefix
    prefix = f"metadata/parquet/year={year}/court={court_code}/bench={bench}"

    # 1. List parquet metadata file in the S3 bucket under the prefix + bench part
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=prefix)

    parquet_key = None
    for obj in response.get("Contents", []):
        if obj["Key"].endswith("metadata.parquet"):
            parquet_key = obj["Key"]
            break

    if not parquet_key:
        print(f"No metadata.parquet found under prefix {prefix}")
        return

    # 2. Download parquet metadata
    parquet_obj = s3.get_object(Bucket=source_bucket, Key=parquet_key)
    parquet_bytes = parquet_obj["Body"].read()

    # 3. Load metadata
    metadata_records = load_metadata_from_parquet(parquet_bytes)
    
    # Get court name from first record
    court_name = "unknown-court"
    if metadata_records:
        court = metadata_records[0].get('court', 'unknown-court')
        court_name = court.replace(' ', '-')
    
    print(f"Found {len(metadata_records)} records to process for {court_name} year {year}")

    # 4. Initialize processing statistics
    processing_stats = {
        'court_name': court_name,
        'year': year,
        'bench': bench,
        'court_code': court_code,
        'process_id': PROCESS_ID,
        'processing_date': datetime.now().isoformat(),
        'start_time': time.time(),
        
        # Document counts
        'total_documents_attempted': 0,
        'successful_documents': 0,
        'failed_documents': 0,
        'empty_text_documents': 0,
        'keys_not_found': 0,
        
        # Content statistics
        'successful_titles': [],
        'token_counts': [],
        'chunk_counts': [],
        
        # # Timing statistics  
        # 'processing_times': [],
        # 'pdf_extraction_times': [],
        # 'embedding_generation_times': [],
        
        # Error counts
        'pdf_extraction_errors': 0,
        'embedding_generation_errors': 0,
        'dynamodb_write_errors': 0,
        's3_upload_errors': 0,
        
        # Error details
        'failed_files': []
    }

    # 5. Process files with thread pool
    loop = asyncio.get_event_loop()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    tasks = []
    for record in metadata_records:
        # Construct pdf key from record info
        decision_date_str = record.get("decision_date")
        year_val = None
        if decision_date_str:
            try:
                if hasattr(decision_date_str, 'year'):
                    year_val = decision_date_str.year
                else:
                    year_val = datetime.strptime(decision_date_str, "%Y-%m-%d").year
            except (ValueError, AttributeError):
                year_val = year  # Fallback to provided year

        pdf_link = record.get("pdf_link")
        filename = None
        if pdf_link:
            filename = os.path.basename(pdf_link)
            if not filename:
                print("Skipping record without filename")
                continue

        pdf_key = f"data/pdf/year={year_val}/court={court_code}/bench={bench}/{filename}"

        # Schedule process_file as thread to avoid blocking
        task = loop.run_in_executor(
            executor,
            process_file,
            source_bucket,
            vector_bucket,
            target_bucket,
            record,
            pdf_key,
            court_name,
            processing_stats,
            year
        )
        tasks.append(task)

    # Await all tasks
    await asyncio.gather(*tasks)
    
    # 6. Calculate final statistics
    end_time = time.time()
    processing_stats['end_time'] = end_time
    processing_stats['total_processing_time_seconds'] = end_time - processing_stats['start_time']
    
    # Calculate averages
    if processing_stats['token_counts']:
        processing_stats['avg_tokens_per_document'] = sum(processing_stats['token_counts']) / len(processing_stats['token_counts'])
        processing_stats['total_tokens'] = sum(processing_stats['token_counts'])
        processing_stats['min_tokens'] = min(processing_stats['token_counts'])
        processing_stats['max_tokens'] = max(processing_stats['token_counts'])
    
    if processing_stats['chunk_counts']:
        processing_stats['avg_chunks_per_document'] = sum(processing_stats['chunk_counts']) / len(processing_stats['chunk_counts'])
        processing_stats['total_chunks'] = sum(processing_stats['chunk_counts'])
    
    # if processing_stats['processing_times']:
    #     processing_stats['avg_processing_time_per_document'] = sum(processing_stats['processing_times']) / len(processing_stats['processing_times'])
    
    # Calculate success rate
    if processing_stats['total_documents_attempted'] > 0:
        processing_stats['success_rate_percent'] = (processing_stats['successful_documents'] / (processing_stats['total_documents_attempted'] - processing_stats['keys_not_found'])) * 100
    
    # 7. Save titles and statistics separately
    titles_key, stats_key = save_titles_and_stats_separately(
        court_name, 
        year, 
        processing_stats['successful_titles'],
        processing_stats
    )
    
    # 8. Print final summary
    print("\n" + "="*50)
    print(f"PROCESSING COMPLETE FOR {court_name.upper()} YEAR {year}")
    print("="*50)
    print(f"Total Documents Attempted: {processing_stats['total_documents_attempted']}")
    print(f"Successfully Processed: {processing_stats['successful_documents']}")
    print(f"Failed Documents: {processing_stats['failed_documents']}")
    print(f"Success Rate: {processing_stats.get('success_rate_percent', 0):.2f}%")
    print(f"Average Tokens per Document: {processing_stats.get('avg_tokens_per_document', 0):.0f}")
    print(f"Total Chunks Created: {processing_stats.get('total_chunks', 0):,}")
    print(f"Total Processing Time: {processing_stats['total_processing_time_seconds']:.2f} seconds")
    print(f"Files saved: {titles_key}, {stats_key}")
    print("="*50)

# Run main async
import argparse

def parse_args():
    parser = argparse.ArgumentParser(
        description="Process High Court judgments and store embeddings/metadata with simple tracking."
    )
    parser.add_argument("--source-bucket", required=True, help="S3 bucket containing Parquet metadata file")
    parser.add_argument("--vector-bucket", required=True, help="S3 bucket for storing vector embeddings")
    parser.add_argument("--target-bucket", required=True, help="S3 bucket of pdfs to process")
    parser.add_argument("--year", required=True, help="Year of judgments to process (e.g., 2017)")
    parser.add_argument("--court-code", required=True, help="High Court code (e.g., 16~20)")
    parser.add_argument("--bench", required=True, help="High Court Bench (e.g., taphc)")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()

    asyncio.run(
        main_async(
            args.source_bucket,
            args.vector_bucket,
            args.target_bucket,
            args.year,
            args.court_code,
            args.bench,
        )
    )