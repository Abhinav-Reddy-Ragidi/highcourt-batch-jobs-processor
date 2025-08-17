#!/bin/bash

# Configuration
TARGET_BUCKET="indian-highcourt-judgments"
COURT_CODE="36_29"
START_YEAR=2006
BATCH_SIZE=5
MAX_PARALLEL_JOBS=6  # Adjust based on your EC2 instance capacity

# Function to process a single year
process_year() {
    local year=$1
    local log_file="processing_${year}.log"
    
    echo "Starting processing for year: $year"
    python doc_processing.py \
        --target-bucket "$TARGET_BUCKET" \
        --year "$year" \
        --court-code "$COURT_CODE" \
        > "$log_file" 2>&1
    
    if [ $? -eq 0 ]; then
        echo "Successfully completed year: $year"
    else
        echo "Failed processing year: $year - check $log_file"
    fi
}

# Export the function so parallel can use it
export -f process_year
export TARGET_BUCKET COURT_CODE

# Generate list of years in batches of 5
current_year=$(date +%Y)
years_to_process=""

for ((year=$START_YEAR; year<=current_year; year+=BATCH_SIZE)); do
    end_year=$((year + BATCH_SIZE - 1))
    if [ $end_year -gt $current_year ]; then
        end_year=$current_year
    fi
    
    for ((y=$year; y<=end_year; y++)); do
        years_to_process="$years_to_process $y"
    done
done

echo "Years to process: $years_to_process"
echo "Starting parallel processing with max $MAX_PARALLEL_JOBS jobs..."

# Run parallel processing
echo $years_to_process | tr ' ' '\n' | parallel -j $MAX_PARALLEL_JOBS process_year

echo "All processing jobs completed!"
echo "Check individual log files: processing_YYYY.log"