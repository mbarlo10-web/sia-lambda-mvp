#!/usr/bin/env python3
"""
Mock data generator for the SIA resume ingestion pipeline.

Generates realistic fake candidate resumes as plain .txt files to simulate
what the real JobDiva pipeline would produce. Dev tool only — not deployed.

Usage:
    python scripts/mock_ingest.py                       # 10 files, today, local only
    python scripts/mock_ingest.py --count 25 --date 2026-03-20
    python scripts/mock_ingest.py --count 5 --upload    # push to S3
"""

import argparse
import os
import random
import string
import textwrap
from datetime import datetime

S3_BUCKET = "sciata-sia-resumes-raw-dev-us-east-2"

# ---------------------------------------------------------------------------
# Name pools
# ---------------------------------------------------------------------------
FIRST_NAMES = [
    "James", "Maria", "David", "Sarah", "Michael", "Jennifer", "Robert",
    "Linda", "Carlos", "Angela", "Wei", "Priya", "Omar", "Fatima", "Yuki",
    "Andre", "Elena", "Hassan", "Chloe", "Raj", "Olivia", "Daniel",
    "Sophia", "Ethan", "Amara", "Lucas", "Naomi", "Samuel", "Aisha", "Noah",
]

LAST_NAMES = [
    "Smith", "Garcia", "Johnson", "Patel", "Williams", "Nguyen", "Brown",
    "Kim", "Martinez", "Chen", "Davis", "Lopez", "Wilson", "Lee", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "White", "Harris", "Robinson",
    "Clark", "Lewis", "Walker", "Hall", "Young", "Allen", "King", "Wright",
]

CITIES = [
    ("Austin", "TX"), ("Dallas", "TX"), ("Houston", "TX"),
    ("Chicago", "IL"), ("New York", "NY"), ("San Francisco", "CA"),
    ("Seattle", "WA"), ("Denver", "CO"), ("Atlanta", "GA"),
    ("Boston", "MA"), ("Miami", "FL"), ("Portland", "OR"),
    ("Phoenix", "AZ"), ("Raleigh", "NC"), ("Minneapolis", "MN"),
]

UNIVERSITIES = [
    "University of Texas at Austin", "Georgia Institute of Technology",
    "University of Illinois at Urbana-Champaign", "Ohio State University",
    "University of Michigan", "Penn State University",
    "University of California, Berkeley", "University of Florida",
    "Arizona State University", "University of Washington",
    "New York University", "Boston University",
]

COMPANIES = [
    "Pinnacle Tech Solutions", "Bridgewater Analytics", "Summit Health Systems",
    "Redstone Financial Group", "Vanguard Data Services", "Clearpoint Consulting",
    "Meridian Software", "Northstar Cloud Inc.", "Apex Digital Labs",
    "Horizon Engineering", "Cobalt Systems", "Trident Networks",
    "Lakeshore Medical Center", "Sterling Bank Corp", "Evergreen Solutions",
]

# ---------------------------------------------------------------------------
# Role templates
# ---------------------------------------------------------------------------
ROLES = {
    "software engineer": {
        "titles": ["Software Engineer", "Senior Software Engineer", "Backend Engineer"],
        "skills": [
            "Python", "Java", "TypeScript", "Go", "SQL", "Bash",
            "AWS Lambda", "EC2", "S3", "DynamoDB", "CloudFormation",
            "Docker", "Kubernetes", "Terraform", "Jenkins",
            "REST APIs", "GraphQL", "Microservices", "Git", "CI/CD",
        ],
        "summary": "experienced software engineer specializing in scalable cloud applications and backend services",
        "bullets": [
            "Designed and deployed microservices handling {n} requests per day",
            "Built CI/CD pipelines reducing deployment time by {pct} percent",
            "Led migration from monolith to event-driven architecture on AWS",
            "Implemented automated testing achieving {pct} percent code coverage",
            "Mentored {n} junior developers through code reviews and pairing",
        ],
        "degree": "Bachelor of Science in Computer Science",
    },
    "devops engineer": {
        "titles": ["DevOps Engineer", "Senior DevOps Engineer", "Site Reliability Engineer"],
        "skills": [
            "AWS", "Azure", "GCP", "Terraform", "Ansible", "CloudFormation",
            "Docker", "Kubernetes", "Helm", "Jenkins", "GitHub Actions",
            "Prometheus", "Grafana", "Datadog", "Linux", "Bash", "Python",
            "Nginx", "HAProxy", "CI/CD Pipelines",
        ],
        "summary": "DevOps engineer focused on infrastructure automation, reliability, and continuous delivery",
        "bullets": [
            "Managed Kubernetes clusters serving {n} production workloads",
            "Reduced infrastructure costs by {pct} percent through right-sizing and spot instances",
            "Automated provisioning of {n} servers using Terraform and Ansible",
            "Designed monitoring dashboards tracking {n} service-level indicators",
            "Achieved {pct} percent uptime SLA across all production services",
        ],
        "degree": "Bachelor of Science in Information Technology",
    },
    "data analyst": {
        "titles": ["Data Analyst", "Senior Data Analyst", "Business Intelligence Analyst"],
        "skills": [
            "SQL", "Python", "R", "Tableau", "Power BI", "Excel",
            "Pandas", "NumPy", "Jupyter", "Looker", "Snowflake",
            "BigQuery", "ETL Pipelines", "Statistical Modeling",
            "A/B Testing", "Data Visualization", "Redshift",
        ],
        "summary": "data analyst with expertise in turning complex datasets into actionable business insights",
        "bullets": [
            "Built dashboards tracking {n} KPIs used by executive leadership",
            "Automated ETL pipelines reducing manual reporting by {pct} percent",
            "Conducted A/B tests leading to {pct} percent improvement in conversion",
            "Analyzed datasets with over {n} million rows to identify revenue trends",
            "Presented quarterly insights to stakeholders across {n} departments",
        ],
        "degree": "Bachelor of Science in Statistics",
    },
    "project manager": {
        "titles": ["Project Manager", "Senior Project Manager", "Technical Program Manager"],
        "skills": [
            "Agile", "Scrum", "Kanban", "Jira", "Confluence", "MS Project",
            "Stakeholder Management", "Risk Assessment", "Budgeting",
            "Resource Planning", "Waterfall", "SAFe", "PMP Certified",
            "Cross-functional Leadership", "Vendor Management",
        ],
        "summary": "PMP-certified project manager with a track record of delivering complex technical programs on time and under budget",
        "bullets": [
            "Managed portfolio of {n} concurrent projects totaling over two million dollars",
            "Reduced project delivery timelines by {pct} percent through Agile adoption",
            "Coordinated cross-functional teams of up to {n} members across three time zones",
            "Implemented risk management framework that prevented {n} critical blockers",
            "Achieved {pct} percent on-time delivery rate across all managed programs",
        ],
        "degree": "Bachelor of Business Administration",
    },
    "registered nurse": {
        "titles": ["Registered Nurse", "Senior Registered Nurse", "Charge Nurse"],
        "skills": [
            "Patient Assessment", "IV Therapy", "Medication Administration",
            "Electronic Health Records", "Epic Systems", "Cerner",
            "Wound Care", "Telemetry Monitoring", "BLS Certified",
            "ACLS Certified", "Patient Education", "Care Planning",
            "Infection Control", "HIPAA Compliance", "Triage",
        ],
        "summary": "registered nurse with extensive experience in acute care and patient advocacy",
        "bullets": [
            "Provided direct care to an average of {n} patients per shift in a high-acuity unit",
            "Reduced medication errors by {pct} percent through improved documentation protocols",
            "Trained {n} new nursing staff on unit-specific procedures and EHR workflows",
            "Collaborated with interdisciplinary teams to develop care plans for complex cases",
            "Maintained {pct} percent patient satisfaction scores across quarterly reviews",
        ],
        "degree": "Bachelor of Science in Nursing",
    },
    "financial analyst": {
        "titles": ["Financial Analyst", "Senior Financial Analyst", "FP&A Analyst"],
        "skills": [
            "Financial Modeling", "Excel", "VBA", "SQL", "Tableau",
            "SAP", "Oracle Financials", "Bloomberg Terminal",
            "Variance Analysis", "Forecasting", "Budgeting",
            "GAAP", "SOX Compliance", "PowerPoint", "Python",
        ],
        "summary": "financial analyst skilled in forecasting, modeling, and data-driven decision support",
        "bullets": [
            "Built financial models supporting {n} million dollars in investment decisions",
            "Reduced monthly close cycle by {pct} percent through process automation",
            "Prepared variance analyses for budgets exceeding {n} million dollars",
            "Delivered quarterly forecasts within {pct} percent accuracy of actuals",
            "Automated {n} recurring reports saving over twenty hours per month",
        ],
        "degree": "Bachelor of Science in Finance",
    },
}

# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

def _rand_phone() -> str:
    area = random.randint(200, 999)
    mid = random.randint(200, 999)
    last = random.randint(1000, 9999)
    return f"({area}) {mid}-{last}"


def _rand_email(first: str, last: str) -> str:
    domains = ["email.com", "gmail.com", "outlook.com", "yahoo.com", "protonmail.com"]
    sep = random.choice([".", "_", ""])
    num = random.choice(["", str(random.randint(1, 99))])
    return f"{first.lower()}{sep}{last.lower()}{num}@{random.choice(domains)}"


def _rand_years() -> tuple:
    """Return (start_year, end_year) for most recent job and previous job."""
    end = random.randint(2023, 2026)
    start1 = end - random.randint(1, 4)
    start2 = start1 - random.randint(2, 5)
    end2 = start1 - 1
    return start1, end, start2, end2


def _fill_bullet(template: str) -> str:
    return template.format(
        n=random.choice([3, 4, 5, 6, 8, 10, 12, 15, 20, 50, 100, 200, 500]),
        pct=random.choice([15, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 95, 98]),
    )


def generate_resume(first: str, last: str, role_key: str) -> str:
    """Generate a realistic plain-text resume (~200 words)."""
    role = ROLES[role_key]
    city, state = random.choice(CITIES)
    title = random.choice(role["titles"])
    email = _rand_email(first, last)
    phone = _rand_phone()
    university = random.choice(UNIVERSITIES)
    grad_year = random.randint(2010, 2020)

    co1, co2 = random.sample(COMPANIES, 2)
    s1, e1, s2, e2 = _rand_years()
    months = ["January", "February", "March", "April", "May", "June",
              "July", "August", "September", "October", "November", "December"]

    skills_sample = random.sample(role["skills"], min(len(role["skills"]), random.randint(8, 12)))
    bullets_sample = random.sample(role["bullets"], min(len(role["bullets"]), 3))

    lines = [
        f"{first} {last}",
        f"{email} | {phone}",
        f"{city}, {state}",
        "",
        "Summary",
        f"Dedicated {role['summary']} with over {random.randint(3, 12)} years of hands-on "
        f"experience. Adept at collaborating with cross-functional teams to deliver impactful "
        f"results in fast-paced environments. Committed to continuous learning and professional growth.",
        "",
        "Skills",
        ", ".join(skills_sample[:6]),
        ", ".join(skills_sample[6:]),
        "",
        "Experience",
        f"{title} — {co1}, {city}, {state}",
        f"{random.choice(months)} {s1} – Present",
    ]
    for b in bullets_sample[:2]:
        lines.append(f"- {_fill_bullet(b)}")

    lines.append("")
    prev_title = role["titles"][0]  # use base title for previous role
    city2, state2 = random.choice(CITIES)
    lines.extend([
        f"{prev_title} — {co2}, {city2}, {state2}",
        f"{random.choice(months)} {s2} – {random.choice(months)} {e2}",
    ])
    for b in bullets_sample[2:]:
        lines.append(f"- {_fill_bullet(b)}")
    # Add a couple more bullets so the resume has enough body
    extra = [b for b in role["bullets"] if b not in bullets_sample]
    for b in extra[:2]:
        lines.append(f"- {_fill_bullet(b)}")

    lines.extend([
        "",
        "Education",
        f"{role['degree']} — {university}, {grad_year}",
    ])

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Generate mock JobDiva candidate resumes")
    parser.add_argument("--count", type=int, default=10, help="Number of resumes to generate (default: 10)")
    parser.add_argument("--date", type=str, default=None, help="Date folder YYYY-MM-DD (default: today)")
    parser.add_argument("--upload", action="store_true", help="Upload to S3 bucket instead of local output")
    args = parser.parse_args()

    date_str = args.date or datetime.utcnow().strftime("%Y-%m-%d")
    count = args.count

    # Generate unique candidate IDs
    candidate_ids = random.sample(range(10000, 100000), count)
    role_keys = list(ROLES.keys())

    s3_client = None
    if args.upload:
        import boto3
        s3_client = boto3.client("s3")

    output_base = os.path.join("mock_output", "jobdiva", date_str)

    generated = []

    for i in range(count):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        cid = candidate_ids[i]
        role_key = role_keys[i % len(role_keys)]

        filename = f"{cid}_{last.lower()}_{first.lower()}.txt"
        s3_key = f"jobdiva/{date_str}/{filename}"

        resume_text = generate_resume(first, last, role_key)

        if args.upload:
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=resume_text.encode("utf-8"),
                ContentType="text/plain",
            )
            print(f"  uploaded s3://{S3_BUCKET}/{s3_key}")
        else:
            local_path = os.path.join(output_base, filename)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "w") as f:
                f.write(resume_text)
            print(f"  wrote {local_path}")

        generated.append({
            "candidate_id": cid,
            "name": f"{first} {last}",
            "role": role_key,
            "key": s3_key,
        })

    # Summary
    print(f"\n{'='*50}")
    print(f"Generated {count} resumes for date {date_str}")
    if args.upload:
        print(f"Destination: s3://{S3_BUCKET}/jobdiva/{date_str}/")
    else:
        print(f"Destination: {output_base}/")
    print(f"Roles: {', '.join(set(g['role'] for g in generated))}")
    print(f"Candidate IDs: {', '.join(str(g['candidate_id']) for g in generated)}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
