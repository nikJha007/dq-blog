# Deployment Notes & Lessons Learned

## CloudFormation Issues Fixed

### 1. DMS VPC Role
**Error:** `The IAM Role arn:aws:iam::xxx:role/dms-vpc-role is not configured properly`

**Fix:** Added `dms-vpc-role` IAM role directly in CloudFormation template with `AmazonDMSVPCManagementRole` managed policy. DMSSubnetGroup now depends on this role.

### 2. PostgreSQL Version
**Error:** `Cannot find version 15.4 for postgres`

**Fix:** Changed to PostgreSQL 16.11 (latest available). Always check available versions:
```bash
aws rds describe-db-engine-versions --engine postgres --query "DBEngineVersions[*].EngineVersion" --output table --region us-east-1
```

### 3. Parameter Group Family Mismatch
**Error:** `The parameter group with DBParameterGroupFamily postgres15 can't be used for this instance. Use postgres16.`

**Fix:** Changed `DBParameterGroup.Family` from `postgres15` to `postgres16` to match the engine version.

### 4. Serial Resource Creation
**Issue:** MSK cluster starts creating even if RDS fails, making rollback slow (MSK takes 20-30 min).

**Fix:** Added `DependsOn: RDSInstance` to MSKCluster so it only starts after RDS succeeds.

### 5. psycopg2 Lambda Layer
**Error:** `User is not authorized to perform: lambda:GetLayerVersion on resource: arn:aws:lambda:us-east-1:898466741470:layer:psycopg2-py311:1`

**Issue:** External public Lambda layers may not be accessible from all accounts.

**Fix:** Create our own Lambda layer via CLI (not CloudFormation) to avoid chicken-and-egg S3 dependency:
- Wheel: `psycopg2_binary-2.9.9-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl`
- Deploy script extracts wheel into `python/` directory structure
- Creates zip and uploads to `s3://<assets-bucket>/layers/psycopg2-layer.zip`
- Uses `aws lambda publish-layer-version` to create the layer
- Uses `aws lambda update-function-configuration` to attach layer to SQLRunnerFunction
- SQLRunnerFunction uses inline ZipFile code (no S3 dependency)

### 6. Lambda Runtime Version
**Issue:** psycopg2 wheel is for Python 3.10 (cp310), but Lambda was using Python 3.11.

**Fix:** Changed SQLRunnerFunction runtime from `python3.11` to `python3.10` to match the wheel.

## Dependency Chain
```
VPC/Networking → RDS → MSK → DMS → Glue
```

## Python Wheels (Glue 4.0 / Python 3.10)
- PyYAML: `PyYAML-6.0.1-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl`
- PyDeequ: `pydeequ-1.2.0-py3-none-any.whl`
- Deequ JAR: `deequ-2.0.4-spark-3.3.jar`

All wheels are pre-downloaded in `src/libs/` - no downloads during deployment.

## EC2 Setup (Amazon Linux 2023)
```bash
sudo dnf install -y git python3 python3-pip curl
```

## Key Versions
| Component | Version |
|-----------|---------|
| PostgreSQL | 16.11 |
| MSK Kafka | 3.5.1 |
| Glue | 4.0 |
| DMS Engine | 3.5.4 |
| Python (Glue) | 3.10 |
| Spark (Glue) | 3.3 |
