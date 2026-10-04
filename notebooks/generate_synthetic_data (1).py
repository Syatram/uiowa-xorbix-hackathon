# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "Faker",
# ]
# ///
# MAGIC %md
# MAGIC # Synthetic Data Generator — Chiropractic Business Hackathon
# MAGIC
# MAGIC Generates a large, comprehensive set of **synthetic** tables for the Xorbix × UW–Madison
# MAGIC Databricks Hackathon. Designed to run end-to-end on **Databricks Free Edition** (serverless).
# MAGIC
# MAGIC ### No PII / PHI / sensitive data
# MAGIC This generator deliberately avoids anything that could be mistaken for real personal or
# MAGIC health information:
# MAGIC - **No names, emails, phone numbers, physical addresses, or SSNs** for patients, leads, or
# MAGIC   referrals — every person-like entity is a synthetic surrogate ID (e.g. `PT0001234`).
# MAGIC - **No exact birthdates** — only a bucketed `age_band` (e.g. `"35-44"`).
# MAGIC - **No diagnosis, medical record, or clinical note fields** — only business/operational
# MAGIC   attributes (status, service type, revenue, channel, etc.).
# MAGIC - City/state on the `locations` table describe the *business's* clinic locations, not any
# MAGIC   individual's home address.
# MAGIC - All data is produced by the [Faker](https://faker.readthedocs.io/) library and Python's
# MAGIC   `random` module with a fixed seed — nothing is sourced from a real system or real people.
# MAGIC
# MAGIC ### Tables produced
# MAGIC | Table | Purpose | Approx. rows |
# MAGIC |---|---|---|
# MAGIC | `locations` | Clinic locations | 20 |
# MAGIC | `providers` | Staff (chiropractors, therapists) | 90 |
# MAGIC | `patients` | Patient/customer roster (no PHI) | 60,000 |
# MAGIC | `leads` | Sales/marketing leads | 45,000 |
# MAGIC | `appointments` | Every booked appointment | 350,000 |
# MAGIC | `visits` | Completed appointments with revenue | ~260,000 |
# MAGIC | `referrals` | Patient-to-lead referrals | 18,000 |
# MAGIC | `marketing_campaigns` | Campaign spend & performance | 260 |
# MAGIC
# MAGIC Row counts are controlled by the constants in the **Configuration** cell — turn them down if
# MAGIC you want a faster first run, or up if you want an even larger dataset.

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType,
)
from faker import Faker
import random
import time
from datetime import date, timedelta

# Reproducible synthetic data
SEED = 42
Faker.seed(SEED)
random.seed(SEED)
fake = Faker()

# Where the tables get written. If your Free Edition workspace's default catalog is not
# named "workspace", change CATALOG below (run `SHOW CATALOGS` in a SQL cell to check).
CATALOG = "workspace"
SCHEMA = "chiro_hackathon"

# Row counts — tune these up/down depending on how large you want the dataset.
NUM_LOCATIONS = 20
NUM_PROVIDERS = 90
NUM_PATIENTS = 60_000
NUM_LEADS = 45_000
NUM_APPOINTMENTS = 350_000
NUM_REFERRALS = 18_000
NUM_CAMPAIGNS = 260

# Chunk size controls how many rows are generated/written at a time, so large tables
# never have to be held entirely in driver memory at once.
CHUNK_SIZE = 25_000

TODAY = date.today()
EARLIEST_HISTORY = TODAY - timedelta(days=365 * 4)  # ~4 years of synthetic history

def weighted_choice(elements, weights):
    return random.choices(elements, weights=weights, k=1)[0]

def write_chunk(df, table_name, first_chunk):
    mode = "overwrite" if first_chunk else "append"
    df.write.format("delta").mode(mode).saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md ## Create the target schema

# COMMAND ----------

try:
    spark.sql(f"USE CATALOG {CATALOG}")
except Exception as e:
    print(f"Could not switch to catalog '{CATALOG}' ({e}). Using the workspace's current default catalog instead.")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE {SCHEMA}")
print(f"Tables will be written to schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md ## 1. Locations

# COMMAND ----------

REGIONS = ["Midwest", "Northeast", "South", "West"]

locations_schema = StructType([
    StructField("location_id", StringType(), False),
    StructField("location_name", StringType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("region", StringType(), False),
    StructField("capacity_patients_per_day", IntegerType(), False),
    StructField("monthly_lease_cost", DoubleType(), False),
    StructField("opened_date", DateType(), False),
])

location_ids = []
location_rows = []
for i in range(1, NUM_LOCATIONS + 1):
    loc_id = f"LOC{i:03d}"
    location_ids.append(loc_id)
    location_rows.append((
        loc_id,
        f"{fake.company()} Chiropractic & Wellness",
        fake.city(),
        fake.state_abbr(),
        fake.random_element(elements=REGIONS),
        fake.random_int(min=20, max=90),
        round(fake.pyfloat(min_value=3000, max_value=16000, right_digits=2), 2),
        fake.date_between(start_date=EARLIEST_HISTORY, end_date=TODAY - timedelta(days=180)),
    ))

locations_df = spark.createDataFrame(location_rows, schema=locations_schema)
write_chunk(locations_df, "locations", first_chunk=True)
print(f"locations: {locations_df.count():,} rows")

# COMMAND ----------

# MAGIC %md ## 2. Providers

# COMMAND ----------

SPECIALTIES = ["Chiropractor", "Massage Therapist", "Physical Therapy Assistant", "Wellness Coach", "Chiropractic Assistant"]
EMPLOYMENT_TYPES = ["Full-Time", "Part-Time", "Contract"]

providers_schema = StructType([
    StructField("provider_id", StringType(), False),
    StructField("location_id", StringType(), False),
    StructField("specialty", StringType(), False),
    StructField("employment_type", StringType(), False),
    StructField("hire_date", DateType(), False),
    StructField("active_flag", BooleanType(), False),
])

provider_ids = []
providers_by_location = {loc: [] for loc in location_ids}
provider_rows = []
for i in range(1, NUM_PROVIDERS + 1):
    prov_id = f"PRV{i:04d}"
    provider_ids.append(prov_id)
    loc_id = fake.random_element(elements=location_ids)
    providers_by_location[loc_id].append(prov_id)
    provider_rows.append((
        prov_id,
        loc_id,
        fake.random_element(elements=SPECIALTIES),
        fake.random_element(elements=EMPLOYMENT_TYPES),
        fake.date_between(start_date=EARLIEST_HISTORY, end_date=TODAY - timedelta(days=30)),
        fake.boolean(chance_of_getting_true=92),
    ))

# Guarantee every location has at least one provider on staff
for loc_id, provs in providers_by_location.items():
    if not provs:
        providers_by_location[loc_id].append(fake.random_element(elements=provider_ids))

providers_df = spark.createDataFrame(provider_rows, schema=providers_schema)
write_chunk(providers_df, "providers", first_chunk=True)
print(f"providers: {providers_df.count():,} rows")

# COMMAND ----------

# MAGIC %md ## 3. Patients (no PHI — surrogate IDs and bucketed attributes only)

# COMMAND ----------

ACQUISITION_SOURCES = ["Organic Search", "Referral Program", "Social Media", "Paid Ads", "Walk-In", "Insurance Directory", "Community Event"]
PATIENT_STATUSES = ["Active", "Lapsed", "Churned"]
AGE_BANDS = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]

patients_schema = StructType([
    StructField("patient_id", StringType(), False),
    StructField("home_location_id", StringType(), False),
    StructField("acquisition_source", StringType(), False),
    StructField("first_visit_date", DateType(), False),
    StructField("tenure_months", IntegerType(), False),
    StructField("status", StringType(), False),
    StructField("age_band", StringType(), False),
    StructField("lifetime_visit_count", IntegerType(), False),
    StructField("churn_risk_score", DoubleType(), False),
])

patient_ids = []
patient_home_location = {}

def build_patient_chunk(start_idx, count):
    rows = []
    for offset in range(count):
        idx = start_idx + offset
        pid = f"PT{idx:07d}"
        patient_ids.append(pid)
        loc_id = fake.random_element(elements=location_ids)
        patient_home_location[pid] = loc_id
        first_visit = fake.date_between(start_date=EARLIEST_HISTORY, end_date=TODAY - timedelta(days=1))
        tenure_months = max(0, (TODAY - first_visit).days // 30)
        status = fake.random_element(elements=PATIENT_STATUSES) if tenure_months > 3 else "Active"
        rows.append((
            pid,
            loc_id,
            fake.random_element(elements=ACQUISITION_SOURCES),
            first_visit,
            tenure_months,
            status,
            fake.random_element(elements=AGE_BANDS),
            fake.random_int(min=0, max=140),
            round(fake.pyfloat(min_value=0, max_value=1, right_digits=3), 3),
        ))
    return rows

first_chunk = True
start_time = time.time()
for start in range(1, NUM_PATIENTS + 1, CHUNK_SIZE):
    count = min(CHUNK_SIZE, NUM_PATIENTS - start + 1)
    chunk_df = spark.createDataFrame(build_patient_chunk(start, count), schema=patients_schema)
    write_chunk(chunk_df, "patients", first_chunk=first_chunk)
    first_chunk = False
    print(f"patients: wrote rows {start:,}-{start + count - 1:,}")
print(f"patients generation took {time.time() - start_time:.1f}s")

# COMMAND ----------

# MAGIC %md ## 4. Leads

# COMMAND ----------

LEAD_SOURCES = ["Website Form", "Phone Inquiry", "Social Media Ad", "Referral", "Walk-In", "Community Event", "Insurance Directory"]
LEAD_STATUSES = ["New", "Contacted", "Qualified", "Converted", "Lost"]
LEAD_STATUS_WEIGHTS = [0.15, 0.25, 0.20, 0.25, 0.15]

leads_schema = StructType([
    StructField("lead_id", StringType(), False),
    StructField("source", StringType(), False),
    StructField("assigned_location_id", StringType(), False),
    StructField("created_date", DateType(), False),
    StructField("status", StringType(), False),
    StructField("first_response_hours", DoubleType(), False),
    StructField("num_touchpoints", IntegerType(), False),
    StructField("converted_flag", BooleanType(), False),
    StructField("converted_patient_id", StringType(), True),
])

lead_ids = []

def build_lead_chunk(start_idx, count):
    rows = []
    for offset in range(count):
        idx = start_idx + offset
        lid = f"LD{idx:07d}"
        lead_ids.append(lid)
        status = weighted_choice(LEAD_STATUSES, LEAD_STATUS_WEIGHTS)
        converted = status == "Converted"
        # Illustrative linkage only — for a real solution, tighten this to your own lead->patient logic.
        converted_patient = fake.random_element(elements=patient_ids) if converted and patient_ids else None
        rows.append((
            lid,
            fake.random_element(elements=LEAD_SOURCES),
            fake.random_element(elements=location_ids),
            fake.date_between(start_date=EARLIEST_HISTORY, end_date=TODAY),
            status,
            round(fake.pyfloat(min_value=0, max_value=96, right_digits=1), 1),
            fake.random_int(min=0, max=12),
            converted,
            converted_patient,
        ))
    return rows

first_chunk = True
start_time = time.time()
for start in range(1, NUM_LEADS + 1, CHUNK_SIZE):
    count = min(CHUNK_SIZE, NUM_LEADS - start + 1)
    chunk_df = spark.createDataFrame(build_lead_chunk(start, count), schema=leads_schema)
    write_chunk(chunk_df, "leads", first_chunk=first_chunk)
    first_chunk = False
    print(f"leads: wrote rows {start:,}-{start + count - 1:,}")
print(f"leads generation took {time.time() - start_time:.1f}s")

# COMMAND ----------

# MAGIC %md ## 5. Appointments and Visits
# MAGIC
# MAGIC Generated together: every appointment gets a row in `appointments`, and every appointment
# MAGIC with status `"Completed"` also produces a matching row in `visits` (with revenue).

# COMMAND ----------

APPT_TYPES = ["Initial Consultation", "Spinal Adjustment", "Follow-Up Adjustment", "Therapeutic Massage", "Re-Evaluation", "Physical Therapy"]
BOOKED_CHANNELS = ["Online", "Phone", "In-Person", "Mobile App"]
APPT_STATUSES = ["Completed", "No-Show", "Cancelled", "Rescheduled"]
APPT_STATUS_WEIGHTS = [0.74, 0.10, 0.11, 0.05]
PAYMENT_TYPES = ["Insurance", "Self-Pay", "Package Plan", "HSA/FSA"]
SERVICE_REVENUE_RANGE = {
    "Initial Consultation": (60, 150),
    "Spinal Adjustment": (45, 110),
    "Follow-Up Adjustment": (40, 95),
    "Therapeutic Massage": (65, 160),
    "Re-Evaluation": (55, 120),
    "Physical Therapy": (70, 180),
}

appointments_schema = StructType([
    StructField("appointment_id", StringType(), False),
    StructField("patient_id", StringType(), False),
    StructField("provider_id", StringType(), False),
    StructField("location_id", StringType(), False),
    StructField("appointment_date", DateType(), False),
    StructField("appointment_type", StringType(), False),
    StructField("booked_channel", StringType(), False),
    StructField("status", StringType(), False),
    StructField("lead_time_days", IntegerType(), False),
])

visits_schema = StructType([
    StructField("visit_id", StringType(), False),
    StructField("appointment_id", StringType(), False),
    StructField("patient_id", StringType(), False),
    StructField("provider_id", StringType(), False),
    StructField("location_id", StringType(), False),
    StructField("visit_date", DateType(), False),
    StructField("service_type", StringType(), False),
    StructField("revenue", DoubleType(), False),
    StructField("payment_type", StringType(), False),
])

visit_counter = 0

def build_appointment_and_visit_chunk(start_idx, count):
    global visit_counter
    appt_rows, visit_rows = [], []
    for offset in range(count):
        idx = start_idx + offset
        aid = f"AP{idx:08d}"
        pid = fake.random_element(elements=patient_ids)
        home_loc = patient_home_location.get(pid, fake.random_element(elements=location_ids))
        # Most visits happen at the patient's home location; some happen elsewhere.
        loc_id = home_loc if random.random() < 0.85 else fake.random_element(elements=location_ids)
        candidates = providers_by_location.get(loc_id) or provider_ids
        prov_id = fake.random_element(elements=candidates)
        appt_date = fake.date_between(start_date=EARLIEST_HISTORY, end_date=TODAY)
        appt_type = fake.random_element(elements=APPT_TYPES)
        status = weighted_choice(APPT_STATUSES, APPT_STATUS_WEIGHTS)

        appt_rows.append((
            aid, pid, prov_id, loc_id, appt_date, appt_type,
            fake.random_element(elements=BOOKED_CHANNELS),
            status,
            fake.random_int(min=0, max=30),
        ))

        if status == "Completed":
            visit_counter += 1
            low, high = SERVICE_REVENUE_RANGE.get(appt_type, (50, 150))
            visit_rows.append((
                f"VS{visit_counter:08d}",
                aid, pid, prov_id, loc_id, appt_date,
                appt_type,
                round(fake.pyfloat(min_value=low, max_value=high, right_digits=2), 2),
                fake.random_element(elements=PAYMENT_TYPES),
            ))
    return appt_rows, visit_rows

first_chunk = True
start_time = time.time()
for start in range(1, NUM_APPOINTMENTS + 1, CHUNK_SIZE):
    count = min(CHUNK_SIZE, NUM_APPOINTMENTS - start + 1)
    appt_rows, visit_rows = build_appointment_and_visit_chunk(start, count)

    appt_df = spark.createDataFrame(appt_rows, schema=appointments_schema)
    write_chunk(appt_df, "appointments", first_chunk=first_chunk)

    if visit_rows:
        visit_df = spark.createDataFrame(visit_rows, schema=visits_schema)
        write_chunk(visit_df, "visits", first_chunk=first_chunk)

    first_chunk = False
    print(f"appointments/visits: wrote appointment rows {start:,}-{start + count - 1:,} "
          f"({len(visit_rows):,} completed visits in this chunk)")
print(f"appointments+visits generation took {time.time() - start_time:.1f}s")

# COMMAND ----------

# MAGIC %md ## 6. Referrals

# COMMAND ----------

REFERRAL_CHANNELS = ["Verbal Referral", "Referral Card", "Digital Share", "Family Referral Program"]
REFERRAL_OUTCOMES = ["Converted", "Pending", "Declined", "Expired"]
REFERRAL_OUTCOME_WEIGHTS = [0.35, 0.25, 0.20, 0.20]

referrals_schema = StructType([
    StructField("referral_id", StringType(), False),
    StructField("referring_patient_id", StringType(), False),
    StructField("referred_lead_id", StringType(), True),
    StructField("referral_date", DateType(), False),
    StructField("channel", StringType(), False),
    StructField("outcome", StringType(), False),
])

def build_referral_chunk(start_idx, count):
    rows = []
    for offset in range(count):
        idx = start_idx + offset
        rid = f"RF{idx:07d}"
        referring_patient = fake.random_element(elements=patient_ids)
        referred_lead = fake.random_element(elements=lead_ids) if lead_ids and random.random() < 0.7 else None
        rows.append((
            rid,
            referring_patient,
            referred_lead,
            fake.date_between(start_date=EARLIEST_HISTORY, end_date=TODAY),
            fake.random_element(elements=REFERRAL_CHANNELS),
            weighted_choice(REFERRAL_OUTCOMES, REFERRAL_OUTCOME_WEIGHTS),
        ))
    return rows

first_chunk = True
start_time = time.time()
for start in range(1, NUM_REFERRALS + 1, CHUNK_SIZE):
    count = min(CHUNK_SIZE, NUM_REFERRALS - start + 1)
    chunk_df = spark.createDataFrame(build_referral_chunk(start, count), schema=referrals_schema)
    write_chunk(chunk_df, "referrals", first_chunk=first_chunk)
    first_chunk = False
    print(f"referrals: wrote rows {start:,}-{start + count - 1:,}")
print(f"referrals generation took {time.time() - start_time:.1f}s")

# COMMAND ----------

# MAGIC %md ## 7. Marketing Campaigns

# COMMAND ----------

CAMPAIGN_CHANNELS = ["Paid Search", "Social Media", "Email", "Direct Mail", "Local Event", "Referral Program", "SEO"]

marketing_schema = StructType([
    StructField("campaign_id", StringType(), False),
    StructField("campaign_name", StringType(), False),
    StructField("channel", StringType(), False),
    StructField("start_date", DateType(), False),
    StructField("end_date", DateType(), False),
    StructField("budget", DoubleType(), False),
    StructField("impressions", IntegerType(), False),
    StructField("clicks", IntegerType(), False),
    StructField("leads_generated", IntegerType(), False),
    StructField("conversions", IntegerType(), False),
])

campaign_rows = []
for i in range(1, NUM_CAMPAIGNS + 1):
    cid = f"MKT{i:04d}"
    start_date = fake.date_between(start_date=EARLIEST_HISTORY, end_date=TODAY - timedelta(days=14))
    end_date = start_date + timedelta(days=fake.random_int(min=7, max=90))
    impressions = fake.random_int(min=5_000, max=500_000)
    clicks = int(impressions * fake.pyfloat(min_value=0.01, max_value=0.12, right_digits=3))
    leads_generated = int(clicks * fake.pyfloat(min_value=0.02, max_value=0.20, right_digits=3))
    conversions = int(leads_generated * fake.pyfloat(min_value=0.05, max_value=0.35, right_digits=3))
    campaign_rows.append((
        cid,
        fake.catch_phrase(),
        fake.random_element(elements=CAMPAIGN_CHANNELS),
        start_date,
        end_date,
        round(fake.pyfloat(min_value=500, max_value=25000, right_digits=2), 2),
        impressions,
        clicks,
        leads_generated,
        conversions,
    ))

marketing_df = spark.createDataFrame(campaign_rows, schema=marketing_schema)
write_chunk(marketing_df, "marketing_campaigns", first_chunk=True)
print(f"marketing_campaigns: {marketing_df.count():,} rows")

# COMMAND ----------

# MAGIC %md ## 8. Validate the generated dataset

# COMMAND ----------

TABLES = ["locations", "providers", "patients", "leads", "appointments", "visits", "referrals", "marketing_campaigns"]

for t in TABLES:
    print(f"{t:>22}: {spark.table(t).count():>10,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional: compact the Delta files
# MAGIC `OPTIMIZE` isn't required, but it can speed up downstream queries after this many small
# MAGIC append writes. Safe to skip if it's unavailable in your workspace/edition.

# COMMAND ----------

for t in TABLES:
    try:
        spark.sql(f"OPTIMIZE {t}")
    except Exception as e:
        print(f"Skipped OPTIMIZE on {t}: {e}")

print("Synthetic data generation complete.")
print(f"Tables are available at {CATALOG}.{SCHEMA}.<table_name> (or <current_catalog>.{SCHEMA}.<table_name>).")