# Streaming ETL Framework with Data Quality

A **config-driven CDC streaming ETL pipeline** on AWS that captures real-time database changes and processes them through a modern data lakehouse architecture with enterprise-grade data quality monitoring using **AWS Deequ**.

**RDS PostgreSQL → DMS CDC → MSK Kafka → Glue Streaming → Delta Lake (S3)**

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
| **Multi-table CDC** | Per-table Kafka topics with automatic schema detection |
| **Config-driven DQ Rules** | `range`, `allowed_values`, `regex` validation rules |
| **Config-driven Transforms** | `trim`, `lower`, `upper`, `round`, `mask_pii` transforms |
| **Deequ Integration** | Enterprise DQ metrics: Completeness, Uniqueness, Compliance, Size |
| **SCD Type 2** | Full history tracking with `_effective_from`, `_effective_to`, `_is_current` |
| **Quarantine** | Failed DQ records isolated with failure reason for analysis |
| **Athena Queryable** | Delta Lake tables registered in Glue Catalog for SQL analytics |
| **Utility Lambdas** | SQL Runner, Kafka Admin, Athena Table Creator for operations |

## Service Versions

| Service | Version | Notes |
|---------|---------|-------|
| **RDS PostgreSQL** | 16.11 | Latest stable, with `wal_level=logical` for CDC |
| **Amazon MSK (Kafka)** | 3.5.1 | SASL/SCRAM authentication on port 9096 |
| **AWS DMS** | 3.5.4 | CDC replication engine |
| **AWS Glue** | 4.0 | Spark 3.3, Python 3.10 |
| **Deequ JAR** | 2.0.4-spark-3.3 | Data quality library for Spark |
| **PyDeequ** | 1.2.0 | Python wrapper for Deequ |
| **PyYAML** | 6.0.1 | Config parsing (cp310 manylinux wheel) |
| **Delta Lake** | 2.4.0 | Included in Glue 4.0 |

## Prerequisites

- **AWS CLI v2** installed and configured
- **AWS Account** with permissions for: CloudFormation, RDS, DMS, MSK, Glue, S3, IAM, Lambda, Secrets Manager, Athena, KMS
- **Python 3.9+** with pip (for downloading dependencies)
- **curl** (for downloading Deequ JAR)

## EC2 Deployment Setup

If deploying from an EC2 instance in a fresh AWS account:

### Option 1: IAM Instance Profile (Recommended)

1. Create an IAM Role with the following policies:
   - `AdministratorAccess` (for demo purposes) OR create a custom policy with:
     - CloudFormation full access
     - RDS, DMS, MSK, Glue, S3, Lambda, Secrets Manager, Athena, KMS full access
     - IAM role creation permissions
     - EC2 network interface permissions

2. Attach the IAM Role to your EC2 instance:
   ```bash
   # From AWS Console: EC2 > Instance > Actions > Security > Modify IAM Role
   # Or via CLI:
   aws ec2 associate-iam-instance-profile \
     --instance-id i-xxxxxxxxxxxx \
     --iam-instance-profile Name=YourInstanceProfileName
   ```

3. Verify credentials work:
   ```bash
   aws sts get-caller-identity
   ```

### Option 2: AWS CLI Configuration

If not using an instance profile, configure AWS CLI:

```bash
# Configure with access keys
aws configure
# Enter: AWS Access Key ID, Secret Access Key, Region (us-east-1), Output format (json)

# Or set environment variables
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"
```

### EC2 Instance Requirements (Amazon Linux 2023)

```bash
# Install required tools
sudo dnf install -y git python3 python3-pip curl

# Verify installations
git --version
python3 --version
aws --version
```

For Ubuntu:
```bash
sudo apt-get update && sudo apt-get install -y git python3 python3-pip curl
```

## Quick Start

```bash
# Clone the repository
git clone https://github.com/nikJha007/dq-blog.git
cd dq-blog

# Make scripts executable
chmod +x scripts/*.sh

# Deploy the entire stack (takes ~30-45 minutes)
./scripts/deploy.sh

# Or deploy with custom stack name
./scripts/deploy.sh --stack-name my-etl-stack --region us-east-1
```

## Project Structure

```
dq-blog/
├── cloudformation/
│   └── streaming-etl.yaml      # One-click CloudFormation template
├── config/
│   ├── tables.yaml             # Table schemas, DQ rules, transforms
│   └── dms_table_mappings.json # DMS table mapping rules
├── scripts/
│   ├── deploy.sh               # One-click deployment script
│   ├── teardown.sh             # Resource cleanup script
│   ├── iot_data_generator.py   # Test data generator Lambda
│   ├── create_tables.sql       # RDS table DDL
│   └── analytics_queries.sql   # Sample Athena queries
├── src/
│   ├── glue_streaming_job.py   # Main Glue streaming job
│   ├── deequ_analyzer.py       # Deequ DQ metrics analyzer (REQUIRED)
│   └── libs/                   # Python wheels for Glue
├── docs/
│   └── architecture.md         # Detailed architecture docs
└── README.md
```

## Deequ Requirements

This framework uses **AWS Deequ** for enterprise-grade data quality metrics. The deploy script automatically downloads and uploads the required libraries:

| File | S3 Path | Description |
|------|---------|-------------|
| `pydeequ-1.2.0-py3-none-any.whl` | `s3://<assets-bucket>/libs/` | PyDeequ Python wheel |
| `deequ-2.0.4-spark-3.3.jar` | `s3://<assets-bucket>/libs/` | Deequ JAR for Spark 3.3 |
| `PyYAML-6.0.1-*.whl` | `s3://<assets-bucket>/libs/` | PyYAML wheel for config parsing (Python 3.10) |
| `deequ_analyzer.py` | `s3://<assets-bucket>/scripts/` | Deequ analyzer module |
| `psycopg2-layer.zip` | `s3://<assets-bucket>/layers/` | psycopg2 Lambda layer for SQL Runner |

The Glue job is configured with:
- `--extra-py-files`: PyYAML wheel, PyDeequ wheel, deequ_analyzer.py
- `--extra-jars`: Deequ JAR file

**Note:** Deequ is REQUIRED - the job will fail if Deequ libraries aren't available. The `deploy.sh` script handles downloading these automatically.

## psycopg2 Lambda Layer

The SQL Runner Lambda requires psycopg2 to connect to PostgreSQL. Instead of using external public layers (which may have access issues), we create our own layer:

- Pre-downloaded wheel: `src/libs/psycopg2_binary-2.9.9-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl`
- Deploy script creates layer zip and uploads to S3
- CloudFormation creates the Lambda Layer from S3
- SQLRunnerFunction uses Python 3.10 runtime to match the wheel

## kafka-python Lambda Layer

The Kafka Admin Lambda requires kafka-python to manage MSK topics:

- Pre-downloaded wheel: `src/libs/kafka_python-2.3.0-py2.py3-none-any.whl`
- Deploy script creates layer zip and uploads to S3
- Layer attached to KafkaAdminFunction post-deploy

## Utility Lambdas

The CloudFormation template includes three utility Lambda functions:

| Lambda | Purpose | Example Invocation |
|--------|---------|-------------------|
| `${STACK}-sql-runner` | Execute SQL against RDS | `{"sql": "SELECT * FROM vehicles LIMIT 5"}` |
| `${STACK}-kafka-admin` | Manage Kafka topics | `{"action": "sync_from_config"}` |
| `${STACK}-athena-table-creator` | Create Athena tables for Delta Lake | `{}` (no payload needed) |

### SQL Runner Examples
```bash
# Create tables
aws lambda invoke --function-name ${STACK}-sql-runner \
  --payload '{"sql": "CREATE TABLE vehicles (id SERIAL PRIMARY KEY, vin VARCHAR(17))"}' \
  --cli-binary-format raw-in-base64-out out.json

# Query data
aws lambda invoke --function-name ${STACK}-sql-runner \
  --payload '{"sql": "SELECT COUNT(*) FROM vehicles"}' \
  --cli-binary-format raw-in-base64-out out.json
```

### Kafka Admin Examples
```bash
# Sync topics from config
aws lambda invoke --function-name ${STACK}-kafka-admin \
  --payload '{"action": "sync_from_config"}' \
  --cli-binary-format raw-in-base64-out out.json

# List topics
aws lambda invoke --function-name ${STACK}-kafka-admin \
  --payload '{"action": "list"}' \
  --cli-binary-format raw-in-base64-out out.json

# Get topic offsets
aws lambda invoke --function-name ${STACK}-kafka-admin \
  --payload '{"action": "offsets", "topic": "cdc-vehicles"}' \
  --cli-binary-format raw-in-base64-out out.json
```

### Athena Table Creator
```bash
# Create Athena tables for all Delta Lake tables
aws lambda invoke --function-name ${STACK}-athena-table-creator \
  --cli-binary-format raw-in-base64-out out.json
```

## Configuration

### Table Configuration (`config/tables.yaml`)

```yaml
settings:
  checkpoint_location: "s3://your-bucket/checkpoints/"
  quarantine_path: "s3://your-bucket-quarantine/"
  trigger_interval: "15 seconds"

kafka:
  bootstrap_servers: "broker1:9096,broker2:9096"
  security_protocol: "SASL_SSL"
  sasl_mechanism: "SCRAM-SHA-512"
  sasl_username: "kafkaadmin"

tables:
  - name: vehicle_telemetry
    topic: cdc-vehicle_telemetry
    delta_path: "s3://your-bucket/vehicle_telemetry/"
    primary_key: id
    schema:
      - {name: id, type: integer, nullable: false}
      - {name: latitude, type: double, nullable: true}
      - {name: speed_kmh, type: double, nullable: true}
    
    # Custom DQ rules (quarantine failures)
    dq_rules:
      - {id: tel_001, type: range, column: latitude, params: {min: -90, max: 90}, severity: error}
      - {id: tel_002, type: range, column: speed_kmh, params: {min: 0, max: 350}, severity: error}
    
    # Deequ metrics (track over time)
    deequ_checks:
      - {metric: completeness, column: vehicle_id, threshold: 0.95, severity: warning}
      - {metric: uniqueness, column: id, threshold: 1.0, severity: error}
    
    # Data transforms
    transforms:
      - {id: tel_tx_001, type: round, column: latitude, params: {decimals: 5}, order: 1}
```

### DQ Rules Reference

| Rule Type | Parameters | Example |
|-----------|------------|---------|
| `range` | `min`, `max` | `{type: range, column: speed, params: {min: 0, max: 350}}` |
| `allowed_values` | `values` (list) | `{type: allowed_values, column: status, params: {values: ["active", "inactive"]}}` |
| `regex` | `pattern` | `{type: regex, column: email, params: {pattern: "^[a-z]+@.*"}}` |

### Transform Rules Reference

| Transform Type | Parameters | Example |
|----------------|------------|---------|
| `trim` | - | `{type: trim, column: name}` |
| `lower` | - | `{type: lower, column: email}` |
| `upper` | - | `{type: upper, column: status}` |
| `round` | `decimals` | `{type: round, column: latitude, params: {decimals: 5}}` |
| `mask_pii` | `visible_chars` | `{type: mask_pii, column: phone, params: {visible_chars: 4}}` |

## Post-Deployment: Simulating IoT Fleet Data

After deployment completes, run the post-deployment script to set up the database, create Kafka topics, and simulate IoT fleet telemetry data.

### One-Click Post-Deployment Setup

```bash
# Make script executable (if not already)
chmod +x scripts/post-deploy.sh

# Run full post-deployment setup
./scripts/post-deploy.sh --stack-name dq-etl-v7

# This will:
# 1. Create RDS tables (vehicles, drivers, telemetry, deliveries, alerts)
# 2. Create Kafka topics from config/tables.yaml
# 3. Start DMS CDC replication task
# 4. Start Glue streaming job
# 5. Insert sample test data (including bad data to test DQ quarantine)
```

### Selective Post-Deployment

```bash
# Skip certain steps if already done
./scripts/post-deploy.sh --stack-name dq-etl-v7 --skip-tables --skip-topics

# Just start DMS and Glue
./scripts/post-deploy.sh --stack-name dq-etl-v7 --skip-tables --skip-topics

# Generate continuous telemetry data (Ctrl+C to stop)
./scripts/post-deploy.sh --stack-name dq-etl-v7 \
  --skip-tables --skip-topics --skip-dms --skip-glue --generate-data
```

### What the Test Data Includes

| Table | Sample Records | DQ Test Cases |
|-------|---------------|---------------|
| `vehicles` | 3 vehicles (Honda, Toyota, VW) | Valid VINs |
| `drivers` | 2 drivers with PII (phone, license) | PII masking test |
| `vehicle_telemetry` | GPS coordinates, speed, fuel | Speed > 350 (DQ fail → quarantine) |
| `deliveries` | 2 deliveries with status | Status transform (uppercase) |
| `alerts` | 2 alerts (speeding, low fuel) | Alert type validation |

### Verify the Pipeline

```bash
# 1. Check Kafka topics were created
aws lambda invoke --function-name dq-etl-v7-kafka-admin \
  --payload '{"action": "list"}' \
  --cli-binary-format raw-in-base64-out --region us-east-1 out.json && cat out.json

# 2. Check DMS task status
aws dms describe-replication-tasks \
  --filters Name=replication-task-id,Values=dq-etl-v7-cdc-task \
  --query 'ReplicationTasks[0].Status' --output text --region us-east-1

# 3. Check Glue job status
aws glue get-job-runs --job-name dq-etl-v7-streaming-job \
  --query 'JobRuns[0].JobRunState' --output text --region us-east-1

# 4. Check Delta Lake data in S3
aws s3 ls s3://dq-etl-v7-delta-$(aws sts get-caller-identity --query Account --output text)/ --recursive --region us-east-1

# 5. Check quarantine bucket for DQ failures
aws s3 ls s3://dq-etl-v7-quarantine-$(aws sts get-caller-identity --query Account --output text)/ --recursive --region us-east-1
```

### Query Results in Athena

```sql
-- Check vehicles table
SELECT * FROM dq_etl_v7_db.vehicles;

-- Check telemetry with SCD2 columns
SELECT id, vehicle_id, speed_kmh, _effective_from, _is_current 
FROM dq_etl_v7_db.vehicle_telemetry;

-- Check DQ metrics
SELECT * FROM dq_etl_v7_db.dq_metrics ORDER BY timestamp DESC;
```

## Cleanup

```bash
# Delete all resources (with confirmation)
./scripts/teardown.sh

# Force delete without confirmation
./scripts/teardown.sh --force
```

## Cost Estimate

| Resource | Estimated Cost | Notes |
|----------|---------------|-------|
| **MSK (2 brokers)** | ~$200/month | kafka.t3.small instances |
| **RDS PostgreSQL** | ~$15/month | db.t3.micro |
| **Glue Streaming** | ~$50/month | 2 DPUs @ $0.44/DPU-hour (when running) |
| **DMS Replication** | ~$30/month | dms.t3.small |
| **S3 Storage** | ~$5/month | Varies with data volume |
| **NAT Gateway** | ~$35/month | $0.045/hour + data transfer |

**💡 Tip:** Stop the Glue job when not testing. Use `./scripts/teardown.sh` when done.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| **"Unable to locate credentials"** | Configure AWS CLI or attach IAM role to EC2 instance |
| **"Access Denied" on CloudFormation** | Ensure IAM role has CloudFormation and IAM permissions |
| **DMS task fails** | Check security groups, verify RDS credentials in Secrets Manager |
| **No data in Kafka** | Ensure DMS task is running, check table mappings |
| **Glue job fails** | Check CloudWatch logs, verify MSK bootstrap servers |
| **Empty Delta tables** | Verify Kafka topics have data, check checkpoints |
| **PyDeequ download fails** | Ensure pip is installed: `sudo yum install python3-pip` |
| **Deequ JAR download fails** | Ensure curl is installed: `sudo yum install curl` |

### Common EC2 Issues

```bash
# If pip download fails with SSL errors
pip install --upgrade pip certifi

# If AWS CLI not found
sudo yum install -y awscli  # Amazon Linux
# or
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip && sudo ./aws/install

# Check IAM role attached to EC2
curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/
```

## License

MIT License - see [LICENSE](LICENSE) file.
