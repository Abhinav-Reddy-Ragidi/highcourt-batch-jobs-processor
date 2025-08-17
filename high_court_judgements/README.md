# High Court Judgements PDF Processing

This project provides a pipeline for processing PDF documents from Indian High Courts. It downloads PDFs from an S3 bucket, extracts text, performs OCR if necessary, and uploads the processed PDFs back to S3. The processing is parallelized to handle large volumes of documents efficiently.

## Project Structure

- `doc_processing.py`: Contains the main logic for processing PDF documents.
- `Dockerfile`: Defines the environment for the application.
- `requirements.txt`: Lists the Python packages required for the project.
- `README.md`: Documentation for the project.

## Getting Started

### Prerequisites

- Docker installed on your machine.
- AWS credentials configured for accessing S3.

### Building the Docker Image

To build the Docker image, navigate to the project directory and run:

```
docker build -t high_court_judgements .
```

### Running the Docker Container

To run the Docker container, use the following command, replacing the placeholders with your actual values:

```
docker run --rm high_court_judgements --source-bucket <your-source-bucket> --target-bucket <your-target-bucket> --year <year> --court-code <court-code> --workers <number-of-workers> --min-word-ratio <min-word-ratio> --allowed-langs <allowed-languages>
```

### Example

```
docker run --rm high_court_judgements --source-bucket indian-high-court-judgments --target-bucket my-processed-bucket --year 2023 --court-code ABC --workers 4 --min-word-ratio 0.6 --allowed-langs en,hi,te,mr
```

## Logging

Logs will be generated in JSON format and can be found in the specified output bucket.

## Dependencies

The project requires the following Python packages:

- `boto3`: For interacting with AWS S3.
- `PyPDF2`: For PDF text extraction.
- `langdetect`: For language detection.
- `ocrmypdf`: For performing OCR on PDFs.
- `wordfreq`: For computing word frequency ratios.

## License

This project is licensed under the MIT License.