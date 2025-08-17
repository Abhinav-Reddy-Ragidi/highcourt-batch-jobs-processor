FROM python:3.9-slim


# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

COPY download_nltk.py .
RUN python download_nltk.py

# Copy the rest of the application code
COPY . .

# Command to run the application
ENTRYPOINT ["python", "pipeline.py"]