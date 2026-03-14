import io
import json
import os
import logging
from urllib.parse import unquote_plus
from datetime import datetime, timezone

import boto3

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients (patched in _run_local() when SIMULATE_LOCAL=1)
s3 = boto3.client("s3")

# Environment variables
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "sciata-sia-resumes-processed-dev-us-east-2")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "processed/jobdiva/normalized/")
KMS_KEY_ARN = os.environ.get("KMS_KEY_ARN", "")  # optional but recommended

SUPPORTED_EXTENSIONS = [".txt"]


def build_output_key(source_key: str) -> str:
    """
    Convert e.g. raw/jobdiva/test_resume.txt
    into processed/jobdiva/normalized/test_resume.json
    (path under prefix is flattened to filename only).
    """
    filename = source_key.split("/")[-1]
    base_name = filename.rsplit(".", 1)[0] if "." in filename else filename
    return f"{OUTPUT_PREFIX}{base_name}.json"


def extract_text_from_txt(bucket: str, key: str) -> str:
    """
    Read plain text file from S3 and return decoded UTF-8 text.
    """
    response = s3.get_object(Bucket=bucket, Key=key)
    raw_bytes = response["Body"].read()
    return raw_bytes.decode("utf-8")


def parse_basic_resume_fields(text: str) -> dict:
    """
    Very light MVP parsing.
    - first non-empty line becomes candidate_name
    - rest stays in raw_text
    """
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    candidate_name = lines[0] if lines else "Unknown"

    return {
        "candidate_name": candidate_name,
        "line_count": len(lines),
        "char_count": len(text),
    }


def put_json_to_s3(bucket: str, key: str, payload: dict) -> None:
    """
    Write JSON payload to S3 with optional SSE-KMS encryption.
    """
    body = json.dumps(payload, indent=2).encode("utf-8")

    put_args = {
        "Bucket": bucket,
        "Key": key,
        "Body": body,
        "ContentType": "application/json",
    }

    if KMS_KEY_ARN:
        put_args["ServerSideEncryption"] = "aws:kms"
        put_args["SSEKMSKeyId"] = KMS_KEY_ARN

    s3.put_object(**put_args)


def process_record(record: dict) -> dict:
    """
    Process one S3 event record.
    """
    source_bucket = record["s3"]["bucket"]["name"]
    source_key = unquote_plus(record["s3"]["object"]["key"])

    logger.info(f"Triggered by bucket={source_bucket}, key={source_key}")

    extension = "." + source_key.rsplit(".", 1)[-1].lower() if "." in source_key else ""

    if extension not in SUPPORTED_EXTENSIONS:
        message = f"Unsupported file type: {extension}. Supported: {SUPPORTED_EXTENSIONS}"
        logger.warning(message)
        return {
            "source_bucket": source_bucket,
            "source_key": source_key,
            "status": "skipped",
            "reason": message,
        }

    text = extract_text_from_txt(source_bucket, source_key)
    logger.info(f"Successfully read file: {source_key}")

    parsed_fields = parse_basic_resume_fields(text)

    output_key = build_output_key(source_key)

    payload = {
        "status": "processed",
        "processed_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_bucket": source_bucket,
        "source_key": source_key,
        "source_file_type": extension.replace(".", ""),
        "parsed_fields": parsed_fields,
        "raw_text": text,
    }

    put_json_to_s3(PROCESSED_BUCKET, output_key, payload)
    logger.info(f"Successfully wrote output to s3://{PROCESSED_BUCKET}/{output_key}")

    return {
        "source_bucket": source_bucket,
        "source_key": source_key,
        "processed_bucket": PROCESSED_BUCKET,
        "processed_key": output_key,
        "status": "success",
    }


def lambda_handler(event, context):
    """
    AWS Lambda entrypoint.
    """
    logger.info("Lambda invoked")
    logger.info(json.dumps(event))

    results = []
    errors = []

    records = event.get("Records", [])
    if not records:
        logger.warning("No records found in event")
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "No S3 records found"})
        }

    for record in records:
        try:
            result = process_record(record)
            results.append(result)
        except Exception as e:
            logger.exception("Error processing record")
            source_bucket = record.get("s3", {}).get("bucket", {}).get("name", "unknown")
            source_key = record.get("s3", {}).get("object", {}).get("key", "unknown")
            errors.append({
                "source_bucket": source_bucket,
                "source_key": source_key,
                "error": str(e),
            })

    if errors:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "One or more records failed",
                "results": results,
                "errors": errors,
            })
        }

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "All records processed successfully",
            "results": results,
        })
    }


def _run_local():
    """Run the handler locally: load test_event.json and optionally mock S3."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    event_path = os.path.join(script_dir, "test_event.json")
    sample_path = os.path.join(script_dir, "sample_resume.txt")
    output_dir = os.path.join(script_dir, "local_output")

    with open(event_path) as f:
        event = json.load(f)

    if os.environ.get("SIMULATE_LOCAL"):
        # Mock S3: read from sample_resume.txt, write to local_output/
        def mock_get_object(**kwargs):
            with open(sample_path, "rb") as f:
                body = f.read()
            return {"Body": io.BytesIO(body)}

        def mock_put_object(**kwargs):
            key = kwargs["Key"]
            body = kwargs["Body"]
            if hasattr(body, "read"):
                body = body.read()
            path = os.path.join(output_dir, key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(body)
            logger.info(f"[SIMULATE] Wrote s3://{kwargs['Bucket']}/{key} -> {path}")

        s3.get_object = mock_get_object
        s3.put_object = mock_put_object
        logger.info("Running with SIMULATE_LOCAL: S3 read from sample_resume.txt, write to local_output/")

    result = lambda_handler(event, None)
    print(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    _run_local()