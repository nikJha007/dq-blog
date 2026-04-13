# Streaming ETL Framework - Architecture

## Overview

This framework implements a config-driven CDC streaming ETL pipeline on AWS that captures real-time database changes and processes them through a modern data lakehouse architecture.

## Data Flow

```
RDS PostgreSQL → DMS CDC → MSK Kafka → Glue Streaming → Delta Lake (S3)
```

### 1. Source Database (RDS PostgreSQL)
- IoT Fleet Management tables: vehicles, drivers, vehicle_telemetry, deliveries, alerts
- Logical replication enabled for CDC capture
- Data generator Lambda creates realistic test data

### 2. Change Data Capture (AWS DMS)
- Full-load and CDC replication
- Per-table Kafka topic routing
- JSON message format with metadata envelope

### 3. Message Streaming (Amazon MSK)
- SASL/SCRAM authentication
- Per-table topics: cdc-vehicles, cdc-drivers, etc.
- Auto-topic creation enabled

### 4. Stream Processing (AWS Glue Streaming)
- Config-driven processing from tables.yaml
- DQ rules: range, allowed_values, regex
- Transforms: trim, lower, upper, round, mask_pii
- Deequ metrics: completeness, uniqueness, compliance, size
- SCD Type 2 with effective dates

### 5. Data Lake (Delta Lake on S3)
- ACID transactions
- Time travel queries
- Partitioned by table
- Quarantine bucket for DQ failures

## Key Features

### Data Quality (DQ)
- **Rule-based validation**: Records failing DQ rules are quarantined
- **Deequ metrics**: Statistical analysis tracked over time
- **Threshold alerting**: Warnings/errors when metrics breach thresholds

### Data Transforms (DT)
- **PII masking**: Phone numbers, license numbers
- **Normalization**: Uppercase status, rounded coordinates
- **Extensible**: Add custom transforms via registry

### SCD Type 2
- **Full history**: All changes preserved
- **Effective dates**: _effective_from, _effective_to
- **Current flag**: _is_current for easy querying
- **Point-in-time queries**: Query state at any timestamp
