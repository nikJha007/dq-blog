# z-blueprint.md

A config-driven CDC streaming ETL pipeline on AWS.
Rows change in PostgreSQL → get captured by DMS → flow through Kafka → get validated, transformed, and merged into Delta Lake by a Glue Streaming job. Everything is driven by one YAML file.

```
RDS PostgreSQL ──→ AWS DMS ──→ Amazon MSK (Kafka) ──→ AWS Glue Streaming ──→ Delta Lake (S3)
     |                |               |                       |                    |
  5 tables      CDC capture     per-table topics      7-step pipeline        SCD2 history
  wal=logical   JSON envelope   SASL/SCRAM auth       DQ + transforms        + quarantine
                                port 9096             + Deequ metrics         + Athena
```

---

## Quick Reference — "I want to…"

| I want to… | Go to… |
|------------|--------|
| Add a new table to the pipeline | `config/tables.yaml` → add table entry, `scripts/post-deploy.sh` → add CREATE TABLE SQL |
| Add a new DQ rule type (e.g. `not_null`) | `src/glue_streaming_job.py` → `DQRegistry.__init__()` → add to `self._rules` dict + implement method |
| Add a new transform type (e.g. `hash`) | `src/glue_streaming_job.py` → `TransformRegistry.__init__()` → add to `self._transforms` dict + implement method |
| Add a new Deequ metric | `src/deequ_analyzer.py` → `_build_analyzers()` → add `elif` block |
| Change DQ rules for an existing table | `config/tables.yaml` → edit `dq_rules` under that table (no code changes) |
| Change transforms for an existing table | `config/tables.yaml` → edit `transforms` under that table (no code changes) |
| Deploy the whole stack from scratch | `./scripts/deploy.sh --stack-name X --region Y` then `./scripts/post-deploy.sh --stack-name X --region Y` |
| Tear everything down | `./scripts/teardown.sh` |
| Understand why a record was quarantined | Check `s3://{stack}-quarantine-{account}/{table}/` — Parquet files with `_failed_rule` and `_quarantined_at` columns |
| Query the processed data | Athena → `SELECT * FROM {stack}_db.{table}` |
| See Deequ DQ metrics over time | Athena → `SELECT * FROM {stack}_db.dq_metrics ORDER BY timestamp DESC` |
| Understand the CloudFormation resources | `cloudformation/streaming-etl.yaml` — single template, all resources |
| See what versions of everything we use | Section "Versions" at the bottom of this file |

---

## The Story of a Row

This is the core of the project. Here's exactly what happens when a row gets inserted/updated/deleted in PostgreSQL, traced through every file and function.

**1. A row changes in RDS PostgreSQL**

The database has `wal_level=logical` enabled. Five tables: `vehicles`, `drivers`, `vehicle_telemetry`, `deliveries`, `alerts`. Tables are created by `scripts/post-deploy.sh` which invokes the `{stack}-sql-runner` Lambda.

**2. DMS captures the change**

AWS DMS (configured in `cloudformation/streaming-etl.yaml`) watches the PostgreSQL WAL. It wraps each changed row in a JSON envelope and sends it to a per-table Kafka topic:

```json
{
  "data": { "id": 1, "speed_kmh": 400.0, "latitude": 40.7128, ... },
  "metadata": { "operation": "insert", "schema-name": "public", "table-name": "vehicle_telemetry" }
}
```

Topic naming: `cdc-{table_name}` (e.g. `cdc-vehicles`, `cdc-vehicle_telemetry`).
Table-to-topic mapping: `config/dms_table_mappings.json`.

**3. Kafka holds the message**

Amazon MSK (Kafka 3.5.1) with SASL/SCRAM-SHA-512 auth on port 9096. Credentials live in Secrets Manager as `AmazonMSK_{stack}_scram`, encrypted with a dedicated KMS key.

**4. Glue picks it up**

`src/glue_streaming_job.py` → `main()` runs at startup:

```
main()
  → load_config_from_s3()        # Loads config/tables.yaml from S3
  │   → Resolves ${STACK_NAME} and ${AWS_ACCOUNT_ID} placeholders
  │   → Derives stack name from bucket name: {stack}-assets-{account}
  │   → Calls get_msk_bootstrap_servers() to resolve Kafka brokers from MSK API
  │
  → get_kafka_password()          # Reads SCRAM password from Secrets Manager
  │
  → create_deequ_analyzer()       # Initializes DeequAnalyzer with S3 state path
  │
  → For each table in config (excluding internal tables):
      → start_stream_for_table()
          → Builds Kafka connection options (SASL/SCRAM JAAS config)
          → spark.readStream.format("kafka").options(**kafka_opts).load()
          → writeStream.foreachBatch(process_batch).start()
```

One Spark Structured Streaming query per table, all running in parallel. Each fires `process_batch()` every 15 seconds (configurable via `settings.trigger_interval`).

**5. The 7-step micro-batch pipeline**

This is the heart of the system. Every micro-batch for every table goes through this exact sequence inside `process_batch()`:

```
process_batch(batch_df, batch_id, table_config, config, spark, deequ_analyzer)
  │
  ├─ Step 1: parse_dms_messages()
  │    Parses the DMS JSON envelope using build_dms_envelope_schema().
  │    Extracts data columns + maps operation (insert/update/delete/load → INSERT/UPDATE/DELETE).
  │    Filters out control messages (rows where all data columns are null).
  │
  ├─ Step 2: deequ_analyzer.analyze_batch()          [src/deequ_analyzer.py]
  │    Runs configured Deequ analyzers (completeness, uniqueness, compliance, size).
  │    Persists state to S3 via FileSystemMetricsRepository.
  │    Writes metrics to Delta Lake at s3://{delta-bucket}/dq_metrics/.
  │    Logs warnings/errors if metrics breach thresholds.
  │    THIS DOES NOT FILTER ROWS — it only measures and records.
  │
  ├─ Step 3: apply_dq_rules()                        → uses DQRegistry
  │    Iterates over table's dq_rules from config.
  │    For each rule, DQRegistry.apply() returns (passed_df, failed_df).
  │    Failed records (severity=error) get tagged with _failed_rule and collected.
  │    → write_to_quarantine() sends failures to s3://{stack}-quarantine-{account}/{table}/
  │    Only passed records continue.
  │
  ├─ Step 4: apply_transforms()                      → uses TransformRegistry
  │    Iterates over table's transforms from config, sorted by `order` field.
  │    TransformRegistry.apply() modifies columns in-place.
  │    Example: mask_pii on phone → "555-0101" becomes "555-****"
  │
  ├─ Step 5: add_scd2_columns()
  │    Adds: _effective_from=now, _effective_to=null, _is_current=true, _ingested_at=now
  │
  ├─ Step 6: deduplicate_by_pk()
  │    Window function over primary_key, ordered by _ingested_at desc.
  │    Keeps only the latest record per PK within this batch.
  │
  └─ Step 7: Delta MERGE (SCD Type 2)
       If Delta table doesn't exist yet → write as new table.
       If it exists:
         a) For UPDATE/DELETE operations:
            MERGE on "target.{pk} = source.{pk} AND target._is_current = true"
            → Sets _is_current=false, _effective_to=now on the old record.
         b) For INSERT/UPDATE operations:
            Append new version with _is_current=true.
       Result: full change history preserved. Query _is_current=true for latest state.
```

**6. Data lands in Delta Lake**

Three S3 buckets (naming: `{stack}-{purpose}-{account}`):
- `{stack}-delta-{account}` — One folder per table (`/vehicles/`, `/drivers/`, etc.) + `/dq_metrics/` + `/deequ-state/`
- `{stack}-quarantine-{account}` — Failed DQ records as Parquet, one folder per table
- `{stack}-assets-{account}` — Scripts, config, wheels, JARs, checkpoints, Lambda layers

**7. Athena queries it**

`scripts/create-athena-tables.sh` (or the `{stack}-athena-table-creator` Lambda) registers Delta tables in the Glue Catalog. Then you query via Athena SQL.

---

## The Config File

`config/tables.yaml` is the single file that drives the entire pipeline. The Glue job loads it from S3 at startup and resolves two placeholders at runtime:
- `${STACK_NAME}` — derived from the S3 bucket name (pattern: `{stack}-assets-{account}`)
- `${AWS_ACCOUNT_ID}` — from STS GetCallerIdentity

A table entry looks like this (using `vehicle_telemetry` as example):

```yaml
- name: vehicle_telemetry              # Used everywhere: logging, Delta path, checkpoint path
  topic: cdc-vehicle_telemetry         # Kafka topic that DMS writes to
  delta_path: "s3://.../"             # Where Delta Lake data goes
  primary_key: id                      # Used for SCD2 merge + dedup

  schema:                              # → build_schema_from_config() → Spark StructType
    - {name: id, type: integer}        #   IMPORTANT: types must match DMS output, not RDS source
    - {name: speed_kmh, type: double}  #   (DMS sends numeric as double, boolean as string, timestamp as string)

  dq_rules:                            # → DQRegistry.apply() per rule → quarantine failures
    - {id: tel_003, type: range, column: speed_kmh, params: {min: 0, max: 350}, severity: error}

  deequ_checks:                        # → DeequAnalyzer.analyze_batch() → metrics to Delta
    - {metric: completeness, column: vehicle_id, threshold: 0.95, severity: warning}

  transforms:                          # → TransformRegistry.apply() per transform, sorted by order
    - {id: tel_tx_001, type: round, column: latitude, params: {decimals: 5}, order: 1}
```

**Config → Code mapping:**

```
tables[].schema          →  build_schema_from_config()  →  Spark StructType for parsing
tables[].dq_rules        →  DQRegistry.apply()          →  (passed_df, failed_df)
tables[].deequ_checks    →  DeequAnalyzer.analyze_batch()→  metrics DataFrame → Delta
tables[].transforms      →  TransformRegistry.apply()   →  modified DataFrame
tables[].primary_key     →  deduplicate_by_pk()          →  Window(partitionBy=pk)
                         →  Delta MERGE condition         →  "target.{pk} = source.{pk}"
settings.trigger_interval→  trigger(processingTime=...)
settings.quarantine_path →  write_to_quarantine() output
kafka.bootstrap_servers  →  "PLACEHOLDER" → resolved at runtime via MSK API
```

**DQ rule types:** `range` (min/max), `allowed_values` (list), `regex` (pattern).
**Transform types:** `trim`, `lower`, `upper`, `round` (decimals), `mask_pii` (visible_chars).
**Deequ metrics:** `completeness`, `uniqueness`, `compliance`, `size`.

---

## Deployment

Two scripts, run in order. First one builds infrastructure, second one sets up the application.

```
STEP 1: ./scripts/deploy.sh --stack-name X --region Y
=========================================================

  check_prerequisites()
       |
       v
  deploy_cloudformation_stack()  ←── cloudformation/streaming-etl.yaml
       |                              Creates: VPC, RDS, MSK, DMS, Glue, S3, Lambdas, IAM, KMS
       |                              Takes ~30-45 min (MSK is the bottleneck)
       |                              Auto-detects existing dms-vpc-role (skips if present)
       v
  upload_assets_to_s3()
       |  Uploads: glue_streaming_job.py, deequ_analyzer.py, tables.yaml
       |  Uploads: PyYAML wheel, PyDeequ wheel, Deequ JAR
       |  Creates + publishes 3 Lambda layers:
       |    psycopg2    → attached to sql-runner Lambda
       |    kafka-python → attached to kafka-admin Lambda
       |    PyYAML      → attached to athena-table-creator Lambda
       v
  (optional: start_dms, start_glue if flags passed)


STEP 2: ./scripts/post-deploy.sh --stack-name X --region Y
============================================================

  create_rds_tables()       ←── Invokes {stack}-sql-runner Lambda with CREATE TABLE SQL
       |
       v
  create_kafka_topics()     ←── Invokes {stack}-kafka-admin Lambda with sync_from_config
       |
       v
  start_dms_task()          ←── Tests source+target connections, then starts replication
       |                         IMPORTANT: tables must exist first or DMS full-load fails
       v
  start_glue_job()          ←── Stops any running job first, then starts fresh
       |
       v
  generate_test_data()      ←── Inserts sample rows including bad data (speed=400 → quarantine)
```

**Why two scripts?** `deploy.sh` creates the AWS resources. But Lambdas need layers (which need S3 which needs the stack), and DMS needs tables to exist. So `post-deploy.sh` handles the chicken-and-egg ordering.

---

## Infrastructure

**CloudFormation dependency chain:**

```
VPC + Subnets + Security Groups
  └→ RDS PostgreSQL
       └→ MSK Cluster (DependsOn: RDS — avoids 30-min rollback if RDS fails)
            └→ MSKBootstrapServers (Custom Resource Lambda — calls GetBootstrapBrokers API)
                 ├→ DMS (Replication Instance + Endpoints + Task)
                 ├→ Glue (Job + Connection)
                 └→ Lambdas (SQL Runner, Kafka Admin, Athena Creator)
```

**Security groups:**
- RDS: port 5432 from VPC CIDR + DMS SG
- MSK: port 9096 from VPC CIDR + DMS SG + Glue SG
- Glue: self-referencing (Glue requirement) + outbound to MSK

**S3 buckets** (all named `{stack}-{purpose}-{account}`):
- `assets` — scripts, config, wheels, JARs, layers, checkpoints
- `delta` — Delta Lake tables, Deequ state, DQ metrics
- `quarantine` — failed DQ records (Parquet)

**Lambdas** (all use inline ZipFile code — no S3 code refs — layers attached post-deploy):

| Lambda | Layer | Purpose |
|--------|-------|---------|
| `{stack}-sql-runner` | psycopg2 | Execute SQL against RDS |
| `{stack}-kafka-admin` | kafka-python + PyYAML | Create/list/manage Kafka topics |
| `{stack}-athena-table-creator` | PyYAML | Create Athena tables over Delta Lake |

**Secrets:**
- `AmazonMSK_{stack}_scram` — Kafka SASL/SCRAM creds (username: `kafkaadmin`)
- Encrypted with dedicated KMS key (`MSKSecretKMSKey`)

---

## Key Design Decisions

| What | Why |
|------|-----|
| Inline Lambda code (ZipFile) | S3 bucket created by same stack — can't ref S3 code that doesn't exist yet |
| Pre-downloaded wheels in `src/libs/` | Zero downloads during deploy — works in air-gapped envs |
| Lambda layers via CLI, not CloudFormation | Avoids circular dep: stack creates S3 → script uploads layer → CLI publishes |
| MSK bootstrap via Custom Resource | CloudFormation doesn't expose BootstrapBrokerStringSaslScram as GetAtt |
| MSK DependsOn RDS | MSK takes 20-30 min — don't want to wait for rollback if RDS fails first |
| Config placeholders resolved at runtime | Same `tables.yaml` works across stacks/accounts/regions |
| Schema types match DMS output, not RDS | DMS converts: numeric→double, boolean→string, timestamp→string |
| SCD2 as two-step merge | Step 1: close old (set _is_current=false). Step 2: append new. Simpler than complex merge. |
| Quarantine = separate S3 bucket | Failed records isolated for analysis, don't pollute Delta tables |
| CreateDMSVPCRole parameter | dms-vpc-role is account-global — deploy.sh auto-detects if it exists |

---

## Versions

| Component | Version | Notes |
|-----------|---------|-------|
| RDS PostgreSQL | 16.11 | wal_level=logical for CDC |
| Amazon MSK | 3.5.1 | SASL/SCRAM on port 9096 |
| AWS DMS | 3.5.4 | CDC replication engine |
| AWS Glue | 4.0 | Spark 3.3, Python 3.10 |
| Deequ JAR | 2.0.4-spark-3.3 | Must match Glue's Spark version |
| PyDeequ | 1.2.0 | Python wrapper |
| PyYAML | 6.0.1 | cp310 manylinux wheel |
| Delta Lake | 2.4.0 | Included in Glue 4.0 |
| psycopg2-binary | 2.9.9 | cp310 wheel for Lambda |
| kafka-python | 2.3.0 | For Kafka Admin Lambda |

---

## File Map

```
dq-blog/
├── src/
│   ├── glue_streaming_job.py       # THE main file — DQRegistry, TransformRegistry, process_batch(), main()
│   ├── deequ_analyzer.py           # DeequAnalyzer class — stateful DQ metrics
│   └── libs/                       # Pre-downloaded wheels + JARs (committed to repo)
│       ├── PyYAML-6.0.1-cp310-*.whl
│       ├── pydeequ-1.2.0-*.whl
│       ├── deequ-2.0.4-spark-3.3.jar
│       ├── psycopg2_binary-2.9.9-cp310-*.whl
│       └── kafka_python-2.3.0-*.whl
├── config/
│   ├── tables.yaml                 # THE config file — schema, DQ rules, transforms, Deequ checks
│   └── dms_table_mappings.json     # DMS table selection rules
├── cloudformation/
│   └── streaming-etl.yaml          # Single CloudFormation template — all AWS resources
├── scripts/
│   ├── deploy.sh                   # Step 1: create stack + upload assets
│   ├── post-deploy.sh              # Step 2: create tables, topics, start services, test data
│   ├── teardown.sh                 # Delete everything
│   ├── create-athena-tables.sh     # Register Delta tables in Glue Catalog
│   ├── create_tables.sql           # RDS DDL (also embedded in post-deploy.sh)
│   ├── analytics_queries.sql       # Sample Athena queries
│   └── iot_data_generator.py       # Test data generator
└── docs/
    ├── architecture.md
    └── deployment-notes.md         # Historical issues fixed during development
```
