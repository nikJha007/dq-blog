# Security

## Reporting a Vulnerability

If you discover a potential security issue in this project, we ask that you notify AWS Security
via our [vulnerability reporting page](https://aws.amazon.com/security/vulnerability-reporting/).
Please do **not** create a public GitHub issue.

## Security Design

This framework deploys a CDC streaming ETL pipeline across multiple AWS services. The following
security controls are implemented:

### Authentication and Secrets
- Amazon RDS and Amazon MSK credentials stored in AWS Secrets Manager
- Secrets encrypted with a customer-managed AWS KMS key (rotation enabled)
- IAM database authentication enabled on Amazon RDS
- Amazon MSK uses SASL/SCRAM-SHA-512 with TLS enforcement

### Network Isolation
- All services deployed in private subnets within a dedicated VPC
- Amazon RDS is not publicly accessible
- Security groups use least-privilege rules with security group references (no broad CIDRs)
- VPC Flow Logs enabled for network monitoring

### Encryption
- S3 buckets encrypted with SSE-KMS (customer-managed key)
- Amazon RDS storage encryption with KMS
- Amazon MSK in-cluster and client-broker TLS encryption
- Amazon SNS topic encrypted with KMS

### Access Control
- Per-Lambda IAM roles with scoped permissions
- S3 bucket policies enforce TLS via `aws:SecureTransport` condition
- S3 Block Public Access enabled on all buckets
- No hardcoded credentials in code or templates

### Logging and Monitoring
- S3 access logging via dedicated logging bucket
- S3 versioning enabled on all buckets
- Amazon MSK broker logs to Amazon CloudWatch
- VPC Flow Logs to Amazon CloudWatch
- Lambda concurrency limits to prevent runaway execution

## Accepted Security Debt

The following items are documented as accepted trade-offs for this sample code:

| # | Item | Rationale |
|---|------|-----------|
| 1 | `Resource: '*'` on EC2 ENI Describe actions (Glue, Lambda roles) | AWS IAM does not support resource-level permissions for `ec2:DescribeNetworkInterfaces` and similar Describe actions |
| 2 | `Resource: '*'` on `secretsmanager:ListSecrets` | ListSecrets does not support resource-level permissions |
| 3 | `Resource: '*'` on Kafka read-only actions (`kafka:ListClusters`) | ListClusters does not support resource-level permissions |
| 4 | `Resource: '*'` on `logs:CreateLogGroup` (bootstrap Lambda) | Custom resource log group name is unknown at deploy time |
| 5 | Amazon RDS without deletion protection | Acceptable for sample code; enable `DeletionProtection: true` for production |
| 6 | AWS Glue job missing security configuration (CKV_AWS_195) | Recommended for production; optional for sample code |
| 7 | Lambda environment variables not encrypted with KMS (CKV_AWS_173) | Environment variables contain ARNs and hostnames, not secrets |

## Production Hardening Recommendations

For production deployments, consider the following additional measures:

- Enable Amazon RDS deletion protection and Multi-AZ (Multi-AZ is enabled in this template)
- Add AWS Glue security configuration for job bookmark encryption
- Encrypt Lambda environment variables with a customer-managed KMS key
- Add AWS Network Firewall or VPC endpoint policies for additional network controls
- Enable AWS CloudTrail for API-level auditing
- Implement automated secret rotation for Amazon RDS and Amazon MSK credentials
- Add AWS WAF if exposing any API endpoints
- Review and tighten IAM policies for your specific use case
