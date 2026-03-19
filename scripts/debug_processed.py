#!/usr/bin/env python3
"""
Debug tool for inspecting processed candidate JSONs in S3.

Reads from the PROCESSED_BUCKET and shows exactly what the Lambda extracted,
what the ranking pipeline would see, and simulates scoring against a job query.

Usage:
    # List all processed candidates for a date
    python scripts/debug_processed.py --date 2026-03-18

    # Inspect a specific candidate by ID
    python scripts/debug_processed.py --date 2026-03-18 --candidate-id 13457035480512

    # Show all candidates and simulate scoring against a job description
    python scripts/debug_processed.py --date 2026-03-18 --query "Python AWS Kubernetes engineer"

    # Scan multiple days
    python scripts/debug_processed.py --days 7

    # Use a specific bucket
    python scripts/debug_processed.py --bucket my-bucket --date 2026-03-18

    # Dump a single candidate's full JSON
    python scripts/debug_processed.py --date 2026-03-18 --candidate-id 13457035480512 --raw
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import boto3

PROCESSED_BUCKET = os.environ.get(
    "PROCESSED_BUCKET", "sciata-sia-resumes-processed-dev-us-east-2"
)

STOPWORDS = frozenset(
    {"a", "the", "and", "or", "in", "of", "to", "for", "with", "is", "are", "be", "that", "this"}
)


def _unique_words(text: str) -> set[str]:
    return {w for w in text.lower().split() if w.isalpha() and w not in STOPWORDS}


def fetch_candidates(s3, bucket: str, dates: list[str]) -> list[dict]:
    """Fetch all processed candidate JSONs for the given dates."""
    candidates = []
    for date_str in dates:
        prefix = f"processed/jobdiva/{date_str}/"
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue
                try:
                    resp = s3.get_object(Bucket=bucket, Key=key)
                    data = json.loads(resp["Body"].read().decode("utf-8"))
                    data["_s3_key"] = key
                    data["_s3_size"] = obj.get("Size", 0)
                    data["_s3_last_modified"] = str(obj.get("LastModified", ""))
                    candidates.append(data)
                except Exception as e:
                    print(f"  ERROR reading {key}: {e}", file=sys.stderr)
    return candidates


def print_candidate_summary(rec: dict, index: int) -> None:
    """Print a concise summary of one candidate record."""
    print(f"\n{'='*70}")
    print(f"  #{index + 1}  {rec.get('candidate_name', 'UNKNOWN')}")
    print(f"{'='*70}")

    print(f"  Candidate ID:    {rec.get('candidate_id', 'N/A')}")
    print(f"  Status:          {rec.get('status', 'MISSING')}")
    print(f"  Ingestion Date:  {rec.get('ingestion_date', 'N/A')}")
    print(f"  Location:        {rec.get('location', 'N/A')}")
    print(f"  Email:           {rec.get('email', 'N/A')}")
    print(f"  Phone:           {rec.get('phone', 'N/A')}")
    print(f"  Available:       {rec.get('available', 'N/A')}")
    print(f"  Schema Version:  {rec.get('schema_version', 'N/A')}")
    print(f"  Source:          {rec.get('source', 'N/A')}")
    print(f"  Text Source:     {rec.get('text_source', 'N/A')}")
    print(f"  Original File:   {rec.get('original_filename', 'N/A')}")
    print(f"  S3 Key:          {rec.get('_s3_key', 'N/A')}")
    print(f"  S3 Size:         {rec.get('_s3_size', 0)} bytes")

    # Qualifications
    quals = rec.get("qualifications", [])
    if quals:
        if isinstance(quals[0], dict):
            qual_names = [q.get("name", "") or q.get("qualificationName", "") for q in quals]
        else:
            qual_names = quals
        print(f"\n  Qualifications ({len(qual_names)}):")
        for q in qual_names:
            print(f"    - {q}")
    else:
        print(f"\n  Qualifications:  NONE")

    # Resume text preview
    resume_text = rec.get("resume_text", "")
    if resume_text:
        preview = resume_text[:500].replace("\n", "\n    ")
        print(f"\n  Resume Text ({len(resume_text)} chars):")
        print(f"    {preview}")
        if len(resume_text) > 500:
            print(f"    ... ({len(resume_text) - 500} more chars)")
    else:
        print(f"\n  Resume Text:     EMPTY / MISSING")

    # Potential issues
    issues = []
    if not resume_text:
        issues.append("NO RESUME TEXT — will be skipped by scoring")
    if not quals:
        issues.append("NO QUALIFICATIONS — qualification_match will be 0")
    if not rec.get("ingestion_date"):
        issues.append("NO INGESTION DATE — recency will default to 0")
    if rec.get("status") != "processed":
        issues.append(f"STATUS is '{rec.get('status', 'MISSING')}' (not 'processed') — OK after fix, was filtered before")

    if issues:
        print(f"\n  ⚠ Potential Issues:")
        for issue in issues:
            print(f"    - {issue}")


def simulate_scoring(candidates: list[dict], query: str) -> None:
    """Simulate the ranking pipeline scoring against candidates."""
    print(f"\n{'#'*70}")
    print(f"  SCORING SIMULATION")
    print(f"  Query: {query}")
    print(f"{'#'*70}")

    job_words = _unique_words(query)
    print(f"\n  Job keywords: {sorted(job_words)}")

    today = datetime.now(timezone.utc)
    scored = []

    for rec in candidates:
        resume_text = rec.get("resume_text", "")
        if not resume_text:
            print(f"\n  SKIP {rec.get('candidate_name', '?')}: no resume text")
            continue

        resume_text_lower = resume_text.lower()

        # Keyword overlap (substring matching)
        if job_words:
            keyword_hits = []
            keyword_misses = []
            for w in job_words:
                if w in resume_text_lower:
                    keyword_hits.append(w)
                else:
                    keyword_misses.append(w)
            keyword_overlap = len(keyword_hits) / len(job_words)
        else:
            keyword_hits, keyword_misses = [], []
            keyword_overlap = 0.0

        # Qualification match (against resume text)
        qualifications = rec.get("qualifications", [])
        qual_names = []
        if isinstance(qualifications, list):
            for q in qualifications:
                if isinstance(q, dict):
                    qual_names.append(q.get("name", "") or q.get("qualificationName", ""))
                elif isinstance(q, str):
                    qual_names.append(q)

        matched_quals = [qn for qn in qual_names if qn and qn.lower() in resume_text_lower]
        unmatched_quals = [qn for qn in qual_names if qn and qn.lower() not in resume_text_lower]
        qualification_match = len(matched_quals) / len(qual_names) if qual_names else 0.0

        # Recency
        ingestion_date_str = rec.get("ingestion_date", "")
        try:
            ingestion_date = datetime.strptime(ingestion_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            days_old = (today - ingestion_date).days
        except (ValueError, TypeError):
            days_old = 999
        recency = max(0.0, 1.0 - (days_old / 365))

        final_score = (0.5 * keyword_overlap) + (0.3 * qualification_match) + (0.2 * recency)

        scored.append({
            "name": rec.get("candidate_name", "?"),
            "id": rec.get("candidate_id", "?"),
            "final_score": round(final_score, 4),
            "keyword_overlap": round(keyword_overlap, 4),
            "qualification_match": round(qualification_match, 4),
            "recency": round(recency, 4),
            "days_old": days_old,
            "keyword_hits": keyword_hits,
            "keyword_misses": keyword_misses,
            "matched_quals": matched_quals,
            "unmatched_quals": unmatched_quals,
        })

    scored.sort(key=lambda x: -x["final_score"])

    if not scored:
        print("\n  No candidates scored (all filtered out).")
        return

    print(f"\n  Scored {len(scored)} candidates:\n")
    for i, s in enumerate(scored):
        print(f"  Rank #{i + 1}: {s['name']} (ID: {s['id']})")
        print(f"    Final Score:         {s['final_score']:.4f} ({int(s['final_score'] * 100)}%)")
        print(f"    Keyword Overlap:     {s['keyword_overlap']:.4f}  hits={s['keyword_hits']}  misses={s['keyword_misses']}")
        print(f"    Qualification Match: {s['qualification_match']:.4f}  matched={s['matched_quals']}  unmatched={s['unmatched_quals']}")
        print(f"    Recency:             {s['recency']:.4f}  ({s['days_old']} days old)")
        print()


def main():
    parser = argparse.ArgumentParser(
        description="Debug tool for inspecting processed candidate JSONs in S3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--bucket", default=PROCESSED_BUCKET, help=f"S3 bucket (default: {PROCESSED_BUCKET})")
    parser.add_argument("--date", default=None, help="Date to inspect (YYYY-MM-DD, default: today)")
    parser.add_argument("--days", type=int, default=1, help="Number of days to scan back (default: 1)")
    parser.add_argument("--candidate-id", default=None, help="Filter to a specific candidate ID")
    parser.add_argument("--query", default=None, help="Simulate scoring against this job description")
    parser.add_argument("--raw", action="store_true", help="Dump full JSON for matched candidates")
    args = parser.parse_args()

    s3 = boto3.client("s3")

    # Build date list
    if args.date:
        base_date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        base_date = datetime.now(timezone.utc)

    dates = []
    for i in range(args.days):
        d = base_date - timedelta(days=i)
        dates.append(d.strftime("%Y-%m-%d"))

    print(f"Scanning bucket: {args.bucket}")
    print(f"Date(s): {', '.join(dates)}")
    print(f"Prefix pattern: processed/jobdiva/<date>/")

    candidates = fetch_candidates(s3, args.bucket, dates)

    if not candidates:
        print(f"\nNo candidates found.")
        sys.exit(0)

    print(f"\nFound {len(candidates)} candidate(s)")

    # Filter by candidate ID if specified
    if args.candidate_id:
        candidates = [c for c in candidates if args.candidate_id in str(c.get("candidate_id", ""))]
        if not candidates:
            print(f"\nNo candidate found matching ID: {args.candidate_id}")
            sys.exit(1)
        print(f"Filtered to {len(candidates)} matching ID: {args.candidate_id}")

    # Raw JSON dump
    if args.raw:
        for c in candidates:
            # Remove debug keys before dumping
            output = {k: v for k, v in c.items() if not k.startswith("_s3_")}
            print(json.dumps(output, indent=2))
        sys.exit(0)

    # Print summaries
    for i, c in enumerate(candidates):
        print_candidate_summary(c, i)

    # Simulate scoring
    if args.query:
        simulate_scoring(candidates, args.query)
    else:
        print(f"\n{'─'*70}")
        print("  TIP: Add --query 'Python AWS engineer' to simulate scoring")
        print(f"{'─'*70}")


if __name__ == "__main__":
    main()
