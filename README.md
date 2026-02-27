# Streaming ETL Framework with Data Quality

Config-driven CDC streaming ETL on AWS. Define tables, DQ rules, transforms, and Deequ checks in a single `tables.yaml` — the framework handles validation, compilation, deployment, and data generation.

**RDS PostgreSQL → DMS (CDC) → MSK (Kafka) → Glue Streaming → Delta Lake (S3)**

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  RDS PostgreSQL │────▶│   AWS DMS       │────▶│   Amazon MSK    │
│  (Source DB)    │ CDC │ (CDC Capture)   │     │ (Kafka Topics)  │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Amazon Athena  │◀────│   Delta Lake    │◀────│  Glue Streaming │
│  (Query Layer)  │     │   (S3 Storage)  │     │  (Processing)   │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                              ┌───────────────────────────┼───────────────────────────┐
                              │                           │                           │
                              ▼                           ▼                           ▼
                     ┌─────────────────┐        ┌─────────────────┐        ┌─────────────────┐
                     │   Quarantine    │        │   DQ Metrics    │        │  SCD2 History   │
                     │   (Failed DQ)   │        │   (Deequ)       │        │  (Delta Lake)   │
                     └─────────────────┘        └─────────────────┘        └─────────────────┘
```

## Features

| Feature | Description |
|---------|-------------|
| **Config-driven pipeline** | One YAML defines tables, schemas, DQ rules, transforms, and Deequ checks |
| **Config validation** | `config_validator.py` validates your YAML before deployment |
| **Config compilation** | `config_compiler.py` generates DMS mappings, RDS DDL, and CFn parameters |
| **DQ rules** | `range`, `allowed_values`, `regex`, `not_null`, `unique`, `length` (extensible via `register()`) |
| **Transforms** | `trim`, `lower`, `upper`, `round`, `mask_pii`, `cast`, `default_value`, `rename` (extensible via `register()`) |
| **Deequ integration** | Completeness, uniqueness, compliance, and size metrics |
| **SCD Type 2** | History tracking with `_effective_from`, `_effective_to`, `_is_current` |
| **Quarantine** | Failed DQ records isolated with failure reason |
| **Isolated stacks** | Each deployment is a fully independent CloudFormation stack |
| **Example use cases** | `vehicle-telemetry` (5 tables) and `healthcare-iot` (2 tables, PII masking) |

## Prerequisites

- AWS CLI v2 configured with appropriate permissions (CloudFormation, RDS, DMS, MSK, Glue, S3, IAM, Lambda, Secrets Manager, Athena, KMS)
- Python 3.10

## Quick Start

```bash
# Clone and prepare
git clone https://github.com/nikJha007/dq-blog.git
cd dq-blog
chmod +x scripts/*.sh

# Deploy (configure region and stack name as needed)
STACK="my-etl-stack"
REGION="us-west-2"

./scripts/deploy.sh --stack-name $STACK --use-case vehicle-telemetry --region $REGION
./scripts/post-deploy.sh --stack-name $STACK --use-case vehicle-telemetry --region $REGION

# Teardown when done
./scripts/teardown.sh --stack-name $STACK --region $REGION --force
```

`deploy.sh` validates and compiles the config, deploys the CloudFormation stack, and uploads all assets to S3. `post-deploy.sh` creates RDS tables, Kafka topics, starts DMS/Glue, creates Athena tables, and seeds test data.

## Project Structure

```
├── cloudformation/
│   └── streaming-etl.yaml              # Parameterized CloudFormation template
├── examples/
│   ├── vehicle-telemetry/
│   │   ├── config/tables.yaml          # 5 tables: vehicles, drivers, telemetry, deliveries, alerts
│   │   └── scripts/
│   │       ├── data_generator.py       # Lambda: seed/generate/burst with DQ violations, SCD2
│   │       └── analytics_queries.sql
│   └── healthcare-iot/
│       ├── config/tables.yaml          # 2 tables: patients, vitals (PII masking, clinical ranges)
│       └── scripts/
│           ├── data_generator.py       # Lambda: seed/generate/burst with PII, clinical DQ
│           └── analytics_queries.sql
├── scripts/
│   ├── deploy.sh                       # Validate, compile, deploy stack, upload assets
│   ├── post-deploy.sh                  # Create tables/topics, start DMS/Glue, seed data
│   ├── teardown.sh                     # Full resource cleanup
│   └── create-athena-tables.sh
├── src/
│   ├── glue_streaming_job.py           # Main Glue streaming job (domain-agnostic)
│   ├── deequ_analyzer.py               # Deequ DQ metrics analyzer
│   ├── config_validator.py             # Config validation
│   ├── config_compiler.py              # Compiles config → DMS mappings, DDL, CFn params
│   ├── test_config_compiler.py         # Unit tests
│   └── libs/                           # Pre-downloaded wheels and JARs
└── docs/
    ├── architecture.md
    └── deployment-notes.md
```

## Configuration

Each use case is defined by a single `tables.yaml`. Here's a condensed example:

```yaml
settings:
  checkpoint_location: "s3://${STACK_NAME}-assets-${AWS_ACCOUNT_ID}/checkpoints/"
  quarantine_path: "s3://${STACK_NAME}-quarantine-${AWS_ACCOUNT_ID}/"
  delta_bucket: "${STACK_NAME}-delta-${AWS_ACCOUNT_ID}"
  trigger_interval: "15 seconds"

kafka:
  bootstrap_servers: "PLACEHOLDER_BOOTSTRAP_SERVERS"
  security_protocol: "SASL_SSL"
  sasl_mechanism: "SCRAM-SHA-512"
  sasl_username: "kafkaadmin"

source_database:
  engine: "postgres"
  schema_name: "public"

tables:
  - name: vehicle_telemetry
    topic: cdc-vehicle_telemetry
    delta_path: "s3://${STACK_NAME}-delta-${AWS_ACCOUNT_ID}/vehicle_telemetry/"
    primary_key: id
    schema:
      - {name: id, type: integer, nullable: false}
      - {name: vehicle_id, type: integer, nullable: true}
      - {name: speed_kmh, type: double, nullable: true}
      - {name: latitude, type: double, nullable: true}

    dq_rules:
      - {id: tel_001, type: range, column: latitude, params: {min: -90, max: 90}, severity: error}
      - {id: tel_002, type: not_null, column: vehicle_id, params: {}, severity: error}

    transforms:
      - {id: tel_tx_001, type: round, column: latitude, params: {decimals: 5}, order: 1}

    deequ_checks:
      - {metric: completeness, column: vehicle_id, threshold: 0.95, severity: warning}
      - {metric: uniqueness, column: id, threshold: 1.0, severity: error}
```

## DQ Rules Reference

| Rule | Parameters | Description |
|------|------------|-------------|
| `range` | `min`, `max` | Value must be within numeric range |
| `allowed_values` | `values` (list) | Value must be in the allowed set |
| `regex` | `pattern` | Value must match the regex pattern |
| `not_null` | — | Value must not be null |
| `unique` | — | Value must be unique within the batch |
| `length` | `min`, `max` | String length must be within range |

Custom rules can be added via `register()` in the DQ engine.

## Transform Rules Reference

| Transform | Parameters | Description |
|-----------|------------|-------------|
| `trim` | — | Strip leading/trailing whitespace |
| `lower` | — | Convert to lowercase |
| `upper` | — | Convert to uppercase |
| `round` | `decimals` | Round numeric value |
| `mask_pii` | `visible_chars` | Mask all but last N characters |
| `cast` | `to_type` | Cast column to a different type |
| `default_value` | `value` | Fill nulls with a default value |
| `rename` | `new_name` | Rename the column |

Custom transforms can be added via `register()` in the transform engine.

## Creating a New Use Case

1. Create `examples/<your-use-case>/config/tables.yaml` with your table definitions, DQ rules, transforms, and Deequ checks.
2. Optionally create `examples/<your-use-case>/scripts/data_generator.py` for test data generation.
3. Deploy: `./scripts/deploy.sh --stack-name <name> --use-case <your-use-case> --region <region>`

## Service Versions

| Service | Version |
|---------|---------|
| RDS PostgreSQL | 16.11 |
| Amazon MSK (Kafka) | 3.5.1 |
| AWS DMS | 3.5.4 |
| AWS Glue | 4.0 (Spark 3.3, Python 3.10) |
| Deequ | 2.0.4-spark-3.3 |
| PyDeequ | 1.2.0 |
| Delta Lake | 2.4.0 |

## Cost Estimate

| Resource | Estimated Cost | Notes |
|----------|---------------|-------|
| MSK (2 brokers) | ~$200/month | kafka.t3.small |
| RDS PostgreSQL | ~$15/month | db.t3.micro |
| Glue Streaming | ~$50/month | 2 DPUs @ $0.44/DPU-hour (when running) |
| DMS Replication | ~$30/month | dms.t3.small |
| S3 Storage | ~$5/month | Varies with data volume |
| NAT Gateway | ~$35/month | $0.045/hour + data transfer |

Stop the Glue job when not testing. Use `./scripts/teardown.sh` when done.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Unable to locate credentials | Configure AWS CLI or attach IAM role to EC2 |
| DMS task fails | Check security groups, verify RDS credentials in Secrets Manager |
| No data in Kafka | Ensure DMS task is running, check table mappings |
| Glue job fails | Check CloudWatch logs, verify MSK bootstrap servers |
| Empty Delta tables | Verify Kafka topics have data, check checkpoints |

## License

MIT License — see [LICENSE](LICENSE) file.
