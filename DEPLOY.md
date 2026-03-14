# Deploy to AWS

## Run locally first (recommended)

Catch simple issues before deploying:

```bash
pip install -r requirements.txt
SIMULATE_LOCAL=1 python lambda_function.py
```

- Loads `test_event.json` and runs the handler.
- With `SIMULATE_LOCAL=1`: mocks S3 — reads from `sample_resume.txt`, writes JSON to `local_output/`. No AWS credentials needed.
- Without it: uses real S3 (file must exist at the bucket/key in `test_event.json`).

Or ask Cursor: **“How do I locally simulate this Lambda handler?”**

---

## What you need

- **AWS account** and credentials configured (`aws configure` or env vars).
- **AWS SAM CLI** installed ([install guide](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)).
- **Docker** (for `sam build`/`sam local`; optional for deploy).

## If the buckets don’t exist yet (default)

1. **Build**
   ```bash
   sam build
   ```

2. **Deploy** (first time will prompt for stack name and region)
   ```bash
   sam deploy --guided
   ```
   Accept defaults or set:
   - **Stack name**: e.g. `sia-resume-processor`
   - **Region**: e.g. `us-east-2`
   - **Confirm changes before deploy**: Y if you want a prompt
   - **Allow SAM CLI IAM role creation**: Y
   - **Disable rollback**: N
   - **Save arguments to samconfig.toml**: Y

3. **Test**  
   Upload a `.txt` file to the source bucket (name in stack outputs). The Lambda runs automatically and writes JSON to the processed bucket.

## If the buckets already exist

Bucket names are globally unique. If `sciata-sia-resumes-raw-dev-us-east-2` or `sciata-sia-resumes-processed-dev-us-east-2` already exist, either:

- **Option A – Different names**  
  Deploy with overrides so the template creates new buckets:
  ```bash
  sam build
  sam deploy --parameter-overrides SourceBucketName=your-unique-raw-bucket ProcessedBucketName=your-unique-processed-bucket
  ```

- **Option B – Use existing buckets**  
  Don’t create buckets in the template. In `template.yaml` you would:
  1. Remove the `SourceBucket` and `ProcessedBucket` resources.
  2. Add parameters for existing bucket names and pass them into the function’s env and policies.
  3. Add the S3 trigger and Lambda permission for the existing source bucket (e.g. in the console: S3 → bucket → Properties → Event notifications → Create, or via CLI/CloudFormation).

## After deploy

- **Source bucket** (from stack output): upload `.txt` files here (e.g. under `raw/jobdiva/`).
- **Processed bucket**: JSON output appears under `processed/jobdiva/normalized/<filename>.json`.
- **Logs**: CloudWatch log group `/aws/lambda/<function-name>`.

## Optional: KMS encryption

To use SSE-KMS on the processed bucket:

1. Create a KMS key (or use an existing one).
2. Add parameter `KMSKeyArn` to the template and pass it as env var `KMS_KEY_ARN` to the function.
3. Grant the Lambda role `kms:Decrypt` and `kms:GenerateDataKey` on that key, and set the processed bucket to use that key if desired.
