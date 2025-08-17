# 🚀 AWS Batch Setup for PDF → HTML Conversion with Prebuilt Docker Image

## 1. **Dockerfile**

Since you already have the **pdf2htmlEX Docker base image**, your Dockerfile looks like this:

```dockerfile
FROM pdf2htmlex/pdf2htmlex:0.18.8.rc1-master-20200630-alpine-3.12.0-x86_64

# Copy your processing script
COPY processing_script.py /app/processing_script.py

WORKDIR /app

CMD ["python3", "processing_script.py"]
```

---

## 2. **Build & Push Image to Amazon ECR**

```bash
# Variables
AWS_REGION=ap-south-1
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REPO_NAME=pdf2html-batch

# Create ECR repo
aws ecr create-repository --repository-name $REPO_NAME --region $AWS_REGION

# Authenticate Docker to ECR
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

# Build Docker image
docker build -t $REPO_NAME .

# Tag image
docker tag $REPO_NAME:latest $ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME:latest

# Push to ECR
docker push $ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME:latest
```

---

## 3. **Create AWS Batch Compute Environment (Fargate)**

```bash
aws batch create-compute-environment \
  --compute-environment-name highcourt-fargate-env \
  --type MANAGED \
  --state ENABLED \
  --compute-resources type=FARGATE,maxvCpus=20,subnets=subnet-0534d235c701fbcd8,securityGroupIds=sg-04fa58204175cd73d
```

---

## 4. **Create Job Queue**

```bash
aws batch create-job-queue \
  --job-queue-name highcourt-job-queue \
  --state ENABLED \
  --priority 1 \
  --compute-environment-order order=1,computeEnvironment=highcourt-fargate-env
```

---

## 5. **Create Job Definition**

```bash
aws batch register-job-definition \
  --job-definition-name pdf2html-job-def \
  --type container \
  --container-properties '{
    "image": "'$ACCOUNT_ID'.dkr.ecr.'$AWS_REGION'.amazonaws.com/'$REPO_NAME':latest",
    "vcpus": 2,
    "memory": 4096,
    "command": ["python3", "processing_script.py"],
    "executionRoleArn": "arn:aws:iam::'$ACCOUNT_ID':role/ecsTaskExecutionRole"
  }'
```

⚠️ Replace `ecsTaskExecutionRole` with your IAM role that has access to ECR + S3.

---

## 6. **Submit a Job**

```bash
aws batch submit-job \
  --job-name pdf2html-test-job \
  --job-queue highcourt-job-queue \
  --job-definition pdf2html-job-def
```

---

✅ That’s the **end-to-end setup**. Once submitted, AWS Batch will:

* Pull your Docker image from ECR
* Spin up a Fargate task in the given subnet + SG
* Run your `processing_script.py`
* Finish and release compute automatically

---

PS C:\Users\Lenovo\high_court_judgements> $years = 2019..2025
>> foreach ($year in $years) {
>>     $json = Get-Content job-container-overrides.json | ForEach-Object { $_ -replace "YEAR_PLACEHOLDER", $year } | Out-String
>>     $tmpFile = "overrides-$year.json"
>>     $json | Set-Content $tmpFile
>>     aws batch submit-job `
>>       --job-name "highcourt-job-$year" `
>>       --job-queue highcourt-jobs-processing-queue `
>>       --job-definition highcourt-job-definition `
>>       --container-overrides file://$tmpFile
>> }

the above is since I want to override some properties to be sent to the processing script so that it will resemble the command line arguments as we are sending previously.