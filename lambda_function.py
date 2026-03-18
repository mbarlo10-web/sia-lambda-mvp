import io
import json
import os
import re
import uuid
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
KMS_KEY_ARN = os.environ.get("KMS_KEY_ARN", "")  # optional but recommended

SUPPORTED_EXTENSIONS = [".txt"]

# Pattern: jobdiva/{YYYY-MM-DD}/{candidate_id}_{lastname}_{firstname}.txt (or .json)
KEY_PATTERN = re.compile(
    r"jobdiva/(\d{4}-\d{2}-\d{2})/(\d+)_([^_]+)_([^_.]+)\.\w+$"
)


def build_output_key(source_key: str) -> str:
    """
    Preserve the date folder from the source key.
    Input:  jobdiva/2026-03-16/12345_doe_jane.txt
    Output: processed/jobdiva/2026-03-16/12345_doe_jane.json
    """
    # Strip leading path segments before 'jobdiva/' if present
    idx = source_key.find("jobdiva/")
    if idx >= 0:
        relative = source_key[idx:]
    else:
        relative = source_key

    base, _ = os.path.splitext(relative)
    return f"processed/{base}.json"


def extract_text_from_txt(bucket: str, key: str) -> str:
    """
    Read plain text file from S3 and return decoded UTF-8 text.
    """
    response = s3.get_object(Bucket=bucket, Key=key)
    raw_bytes = response["Body"].read()
    return raw_bytes.decode("utf-8")


def extract_candidate_id(source_key: str) -> str:
    """
    Extract candidate_id from the S3 key.
    Key format: jobdiva/{YYYY-MM-DD}/{candidate_id}_{lastname}_{firstname}.ext
    Falls back to a UUID if the pattern doesn't match.
    """
    match = KEY_PATTERN.search(source_key)
    if match:
        return match.group(2)
    return str(uuid.uuid4())


def extract_ingestion_date(source_key: str) -> str:
    """
    Extract the date folder from the S3 key.
    Returns the date string or today's date as fallback.
    """
    match = KEY_PATTERN.search(source_key)
    if match:
        return match.group(1)
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def parse_resume_fields(text: str) -> dict:
    """
    Regex-based resume parser. Extracts:
    - candidate_name (first non-empty line)
    - email
    - phone
    - location (city, state)
    - skills list (lines under a 'Skills' section header)
    """
    lines = [line.strip() for line in text.splitlines()]
    non_empty = [l for l in lines if l]

    # Candidate name: first non-empty line
    candidate_name = non_empty[0] if non_empty else "Unknown"

    # Email
    email_match = re.search(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", text)
    email = email_match.group(0) if email_match else ""

    # Phone: various US formats
    phone_match = re.search(
        r"(\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}", text
    )
    phone = phone_match.group(0) if phone_match else ""

    # Location: look for City, ST or City, State patterns
    location_match = re.search(
        r"([A-Z][a-zA-Z\s]+),\s*([A-Z]{2})\b", text
    )
    location = f"{location_match.group(1).strip()}, {location_match.group(2)}" if location_match else ""

    # Skills: grab lines after a "Skills" header until the next section header or blank line gap
    skills = []
    section_header_pattern = re.compile(r"^[A-Z][A-Za-z\s]+:?\s*$")
    in_skills = False
    for line in lines:
        if re.match(r"^skills\s*:?\s*$", line, re.IGNORECASE):
            in_skills = True
            continue
        if in_skills:
            if not line:
                break
            if section_header_pattern.match(line) and not re.match(r"^skills", line, re.IGNORECASE):
                break
            # Split comma/semicolon-separated skills on a single line
            for item in re.split(r"[,;|]", line):
                item = item.strip(" -•·*")
                if item:
                    skills.append(item)

    return {
        "candidate_name": candidate_name,
        "email": email,
        "phone": phone,
        "location": location,
        "skills": skills,
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

    parsed = parse_resume_fields(text)
    candidate_id = extract_candidate_id(source_key)
    ingestion_date = extract_ingestion_date(source_key)
    now_utc = datetime.now(timezone.utc).isoformat()

    # Original filename from the key
    original_filename = source_key.split("/")[-1]

    output_key = build_output_key(source_key)

    payload = {
        "schema_version": "1.0",
        "status": "processed",
        "processed_at_utc": now_utc,
        "ingestion_date": ingestion_date,
        "source": "jobdiva",
        "candidate_id": candidate_id,
        "candidate_name": parsed["candidate_name"],
        "email": parsed["email"],
        "phone": parsed["phone"],
        "location": parsed["location"],
        "available": True,
        "resume_text": text,
        "text_source": "txt_upload",
        "qualifications": parsed["skills"],
        "education": [],
        "experience": [],
        "submittal_history": [],
        "original_filename": original_filename,
        "source_bucket": source_bucket,
        "source_key": source_key,
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
