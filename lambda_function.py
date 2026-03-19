import io
import json
import os
import logging
import zipfile
import xml.etree.ElementTree as ET
from urllib.parse import unquote_plus
from datetime import datetime, timezone

import boto3

# Optional PDF support
try:
    from pypdf import PdfReader  # type: ignore[import-not-found]

    PDF_AVAILABLE = True
except Exception:
    PDF_AVAILABLE = False

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

PROCESSED_BUCKET = os.environ.get(
    "PROCESSED_BUCKET",
    "sciata-sia-resumes-processed-dev-us-east-2",
)
OUTPUT_PREFIX = os.environ.get(
    "OUTPUT_PREFIX",
    "processed/jobdiva/normalized/",
)
KMS_KEY_ARN = os.environ.get("KMS_KEY_ARN", "")

SUPPORTED_EXTENSIONS = [".txt", ".docx", ".pdf", ".json"]


def build_output_key(source_key: str) -> str:
    filename = source_key.split("/")[-1]
    base_name = filename.rsplit(".", 1)[0] if "." in filename else filename
    return f"{OUTPUT_PREFIX}{base_name}.json"


def get_object_bytes(bucket: str, key: str) -> bytes:
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def extract_text_from_txt(bucket: str, key: str) -> str:
    raw_bytes = get_object_bytes(bucket, key)
    return raw_bytes.decode("utf-8", errors="replace")


def extract_text_from_docx(bucket: str, key: str) -> str:
    raw_bytes = get_object_bytes(bucket, key)

    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as z:
        xml_bytes = z.read("word/document.xml")

    root = ET.fromstring(xml_bytes)
    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}

    paragraphs = []
    for para in root.findall(".//w:p", ns):
        texts = [node.text for node in para.findall(".//w:t", ns) if node.text]
        if texts:
            paragraphs.append("".join(texts))

    return "\n".join(paragraphs)


def extract_text_from_pdf(bucket: str, key: str) -> str:
    if not PDF_AVAILABLE:
        raise RuntimeError(
            "PDF parsing requested but pypdf is not installed in this Lambda runtime."
        )

    raw_bytes = get_object_bytes(bucket, key)
    reader = PdfReader(io.BytesIO(raw_bytes))

    pages = []
    for page in reader.pages:
        page_text = page.extract_text() or ""
        pages.append(page_text)

    return "\n".join(pages).strip()


def extract_json_payload(bucket: str, key: str) -> dict:
    raw_bytes = get_object_bytes(bucket, key)
    return json.loads(raw_bytes.decode("utf-8"))


def extract_text_by_extension(bucket: str, key: str, extension: str):
    if extension == ".txt":
        return extract_text_from_txt(bucket, key)
    if extension == ".docx":
        return extract_text_from_docx(bucket, key)
    if extension == ".pdf":
        return extract_text_from_pdf(bucket, key)
    if extension == ".json":
        return extract_json_payload(bucket, key)
    raise ValueError(f"Unsupported file type: {extension}")


def parse_basic_resume_fields(text: str) -> dict:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    candidate_name = lines[0] if lines else "Unknown"

    return {
        "candidate_name": candidate_name,
        "line_count": len(lines),
        "char_count": len(text),
    }


def parse_jobdiva_candidate_fields(payload: dict) -> dict:
    candidate_id = str(
        payload.get("id")
        or payload.get("candidateId")
        or payload.get("candidateID")
        or ""
    )

    first_name = payload.get("first name") or payload.get("firstName") or ""
    middle_initial = payload.get("middle initial") or payload.get("middleInitial") or ""
    last_name = payload.get("last name") or payload.get("lastName") or ""

    candidate_name = (
        " ".join(part for part in [first_name, middle_initial, last_name] if part).strip()
        or "Unknown"
    )

    email = payload.get("email", "") or ""
    alternate_email = (
        payload.get("alternate email", "") or payload.get("alternateEmail", "") or ""
    )
    city = payload.get("city", "") or ""
    state = payload.get("state", "") or ""
    country = payload.get("country", "") or ""
    zip_code = payload.get("zipcode", "") or payload.get("zipCode", "") or ""

    qualifications = payload.get("qualifications", [])
    qualification_names = []
    qualification_values = []

    if isinstance(qualifications, list):
        for q in qualifications:
            if isinstance(q, dict):
                q_name = q.get("qualificationName", "") or q.get("name", "")
                q_value = q.get("qualificationValue", "") or q.get("value", "")
                if q_name:
                    qualification_names.append(str(q_name))
                if q_value:
                    qualification_values.append(str(q_value))

    candidate_qualifications = " | ".join(
        [x for x in qualification_names + qualification_values if x]
    )

    profile_parts = [
        candidate_name,
        email,
        alternate_email,
        city,
        state,
        country,
        zip_code,
        candidate_qualifications,
    ]
    candidate_profile = " ".join([p for p in profile_parts if p]).strip()

    return {
        "candidate_id": candidate_id,
        "candidate_name": candidate_name,
        "candidate_profile": candidate_profile,
        "candidate_qualifications": candidate_qualifications,
        "email": email,
        "alternate_email": alternate_email,
        "city": city,
        "state": state,
        "country": country,
        "zip_code": zip_code,
        "qualification_names": qualification_names,
        "qualification_values": qualification_values,
        "char_count": len(json.dumps(payload)),
    }


def put_json_to_s3(bucket: str, key: str, payload: dict) -> None:
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
    source_bucket = record["s3"]["bucket"]["name"]
    source_key = unquote_plus(record["s3"]["object"]["key"])

    logger.info(f"Triggered by bucket={source_bucket}, key={source_key}")

    extension = "." + source_key.rsplit(".", 1)[-1].lower() if "." in source_key else ""

    if extension not in SUPPORTED_EXTENSIONS:
        message = (
            f"Unsupported file type: {extension}. Supported: {SUPPORTED_EXTENSIONS}"
        )
        logger.warning(message)
        return {
            "source_bucket": source_bucket,
            "source_key": source_key,
            "status": "skipped",
            "reason": message,
        }

    try:
        extracted = extract_text_by_extension(source_bucket, source_key, extension)
    except Exception as e:
        message = f"Extraction failed for {source_key}: {str(e)}"
        logger.warning(message)
        return {
            "source_bucket": source_bucket,
            "source_key": source_key,
            "status": "skipped",
            "reason": message,
        }

    logger.info(f"Successfully read file: {source_key}")

    if extension == ".json":
        source_payload = extracted
        parsed_fields = parse_jobdiva_candidate_fields(source_payload)
        raw_text = json.dumps(source_payload, indent=2)

        payload = {
            "status": "processed",
            "processed_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_bucket": source_bucket,
            "source_key": source_key,
            "source_file_type": "json",
            "candidate_id": parsed_fields["candidate_id"],
            "candidate_name": parsed_fields["candidate_name"],
            "candidate_profile": parsed_fields["candidate_profile"],
            "candidate_qualifications": parsed_fields["candidate_qualifications"],
            "parsed_fields": parsed_fields,
            "raw_text": raw_text,
            "source_payload": source_payload,
        }
    else:
        text = extracted
        parsed_fields = parse_basic_resume_fields(text)

        payload = {
            "status": "processed",
            "processed_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_bucket": source_bucket,
            "source_key": source_key,
            "source_file_type": extension.replace(".", ""),
            "candidate_name": parsed_fields.get("candidate_name", "Unknown"),
            "parsed_fields": parsed_fields,
            "raw_text": text,
        }

    output_key = build_output_key(source_key)
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
    logger.info("Lambda invoked")
    logger.info(json.dumps(event))

    results = []
    errors = []

    records = event.get("Records", [])
    if not records:
        logger.warning("No records found in event")
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "No S3 records found"}),
        }

    for record in records:
        try:
            result = process_record(record)
            results.append(result)
        except Exception as e:
            logger.exception("Error processing record")
            source_bucket = record.get("s3", {}).get("bucket", {}).get("name", "unknown")
            source_key = record.get("s3", {}).get("object", {}).get("key", "unknown")
            errors.append(
                {
                    "source_bucket": source_bucket,
                    "source_key": source_key,
                    "error": str(e),
                }
            )

    if errors:
        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "message": "One or more records failed",
                    "results": results,
                    "errors": errors,
                }
            ),
        }

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "All records processed successfully",
                "results": results,
            }
        ),
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
            logger.info(
                f"[SIMULATE] Wrote s3://{kwargs['Bucket']}/{key} -> {path}"
            )

        s3.get_object = mock_get_object
        s3.put_object = mock_put_object
        logger.info(
            "Running with SIMULATE_LOCAL: S3 read from sample_resume.txt, write to local_output/"
        )

    result = lambda_handler(event, None)
    print(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    _run_local()