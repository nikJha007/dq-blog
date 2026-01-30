#!/bin/bash
# =============================================================================
# Streaming ETL Framework - One-Click Deployment Script
# =============================================================================
# Fully automated deployment for the streaming ETL pipeline.
# Deploys CloudFormation stack, uploads assets, creates tables/topics,
# and starts the data pipeline.
#
# Usage: ./deploy.sh [OPTIONS]
#   -h, --help          Show this help message
#   -s, --stack-name    Stack name (default: streaming-etl)
#   -r, --region        AWS region (default: us-east-1)
#   --start-glue        Start Glue streaming job after deployment
#   --skip-stack        Skip CloudFormation deployment (use existing stack)
# =============================================================================

set -euo pipefail

# =============================================================================
# Configuration
# =============================================================================
STACK_NAME="${STACK_NAME:-dq-etl}"
REGION="${AWS_REGION:-us-east-1}"
START_GLUE=false
SKIP_STACK=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# =============================================================================
# Colors and Logging
# =============================================================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

log_info()    { echo -e "${GREEN}[INFO]${NC}    $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}    $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC}   $1"; }
log_step()    { echo -e "${BLUE}[STEP]${NC}    ${BOLD}$1${NC}"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }

# =============================================================================
# Usage Help
# =============================================================================
show_help() {
    echo -e "${BOLD}Streaming ETL Framework - One-Click Deployment${NC}"
    echo ""
    echo -e "${BOLD}USAGE:${NC}"
    echo "    ./deploy.sh [OPTIONS]"
    echo ""
    echo -e "${BOLD}OPTIONS:${NC}"
    echo "    -h, --help          Show this help message"
    echo "    -s, --stack-name    CloudFormation stack name (default: streaming-etl)"
    echo "    -r, --region        AWS region (default: us-east-1)"
    echo "    --start-glue        Start Glue streaming job after deployment"
    echo "    --skip-stack        Skip CloudFormation deployment"
    echo ""
    exit 0
}

# =============================================================================
# Parse Arguments
# =============================================================================
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help) show_help ;;
            -s|--stack-name) STACK_NAME="$2"; shift 2 ;;
            -r|--region) REGION="$2"; shift 2 ;;
            --start-glue) START_GLUE=true; shift ;;
            --skip-stack) SKIP_STACK=true; shift ;;
            *) log_error "Unknown option: $1"; exit 1 ;;
        esac
    done
}

# =============================================================================
# Utility Functions
# =============================================================================
stack_exists() {
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query 'Stacks[0].StackStatus' \
        --output text 2>/dev/null
}

get_stack_output() {
    local output_key="$1"
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query "Stacks[0].Outputs[?OutputKey=='${output_key}'].OutputValue" \
        --output text 2>/dev/null
}

wait_for_stack() {
    local operation="$1"
    local timeout=3600
    local interval=30
    local elapsed=0
    
    log_info "Waiting for stack ${operation}..."
    
    while [ $elapsed -lt $timeout ]; do
        local status
        status=$(stack_exists)
        
        case "$status" in
            CREATE_COMPLETE|UPDATE_COMPLETE)
                log_success "Stack ${operation} completed"
                return 0
                ;;
            CREATE_IN_PROGRESS|UPDATE_IN_PROGRESS)
                echo -ne "\r  Stack status: ${status} (${elapsed}s)    "
                sleep $interval
                elapsed=$((elapsed + interval))
                ;;
            CREATE_FAILED|UPDATE_FAILED|ROLLBACK_COMPLETE|ROLLBACK_FAILED)
                echo ""
                log_error "Stack ${operation} failed: ${status}"
                return 1
                ;;
            *)
                sleep $interval
                elapsed=$((elapsed + interval))
                ;;
        esac
    done
    
    echo ""
    log_error "Timeout waiting for stack"
    return 1
}

# =============================================================================
# Prerequisites Check
# =============================================================================
check_prerequisites() {
    log_step "Checking prerequisites..."
    
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI not found. Please install it first."
        exit 1
    fi
    
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured."
        exit 1
    fi
    
    local required_files=(
        "cloudformation/streaming-etl.yaml"
        "src/glue_streaming_job.py"
        "config/tables.yaml"
    )
    
    for file in "${required_files[@]}"; do
        if [ ! -f "${PROJECT_ROOT}/${file}" ]; then
            log_error "Required file not found: ${file}"
            exit 1
        fi
    done
    
    local account_id
    account_id=$(aws sts get-caller-identity --query Account --output text)
    log_info "AWS Account: ${account_id}"
    log_info "Region: ${REGION}"
    log_success "Prerequisites OK"
}

# =============================================================================
# Deploy CloudFormation Stack
# =============================================================================
deploy_cloudformation_stack() {
    log_step "Deploying CloudFormation stack..."
    
    local template_file="${PROJECT_ROOT}/cloudformation/streaming-etl.yaml"
    local stack_status
    
    stack_status=$(stack_exists || echo "DOES_NOT_EXIST")
    
    if [ "$stack_status" = "DOES_NOT_EXIST" ]; then
        log_info "Creating new stack: ${STACK_NAME}"
        
        aws cloudformation create-stack \
            --stack-name "$STACK_NAME" \
            --template-body "file://${template_file}" \
            --region "$REGION" \
            --capabilities CAPABILITY_NAMED_IAM \
            --parameters \
                ParameterKey=EnvironmentName,ParameterValue="$STACK_NAME" \
            --tags \
                Key=Project,Value=streaming-etl \
            --output text > /tmp/stack-output.txt 2>&1 || {
                log_error "Failed to create stack"
                cat /tmp/stack-output.txt
                exit 1
            }
        
        wait_for_stack "creation"
        
    elif [[ "$stack_status" == *"COMPLETE"* ]] && [[ "$stack_status" != *"ROLLBACK"* ]]; then
        log_info "Stack exists with status: ${stack_status}"
        log_info "Updating stack..."
        
        aws cloudformation update-stack \
            --stack-name "$STACK_NAME" \
            --template-body "file://${template_file}" \
            --region "$REGION" \
            --capabilities CAPABILITY_NAMED_IAM \
            --parameters \
                ParameterKey=EnvironmentName,ParameterValue="$STACK_NAME" \
            --output text > /tmp/stack-output.txt 2>&1 || {
                if grep -q "No updates" /tmp/stack-output.txt; then
                    log_info "No stack updates needed"
                else
                    log_warn "Stack update issue - check /tmp/stack-output.txt"
                fi
            }
        
        local current_status
        current_status=$(stack_exists)
        if [ "$current_status" = "UPDATE_IN_PROGRESS" ]; then
            wait_for_stack "update"
        fi
    else
        log_error "Stack in invalid state: ${stack_status}"
        exit 1
    fi
    
    log_success "CloudFormation stack ready"
}

# =============================================================================
# Upload Assets to S3
# =============================================================================
upload_assets_to_s3() {
    log_step "Uploading assets to S3..."
    
    local assets_bucket
    assets_bucket=$(get_stack_output "AssetsBucket")
    
    if [ -z "$assets_bucket" ]; then
        log_error "Could not get AssetsBucket from stack outputs"
        exit 1
    fi
    
    log_info "Assets bucket: ${assets_bucket}"
    
    # Upload Glue script
    log_info "Uploading Glue streaming script..."
    aws s3 cp "${PROJECT_ROOT}/src/glue_streaming_job.py" \
        "s3://${assets_bucket}/scripts/" --region "$REGION"
    
    # Upload Deequ analyzer
    if [ -f "${PROJECT_ROOT}/src/deequ_analyzer.py" ]; then
        log_info "Uploading Deequ analyzer..."
        aws s3 cp "${PROJECT_ROOT}/src/deequ_analyzer.py" \
            "s3://${assets_bucket}/scripts/" --region "$REGION"
    fi
    
    # Upload config
    log_info "Uploading tables configuration..."
    aws s3 cp "${PROJECT_ROOT}/config/tables.yaml" \
        "s3://${assets_bucket}/config/" --region "$REGION"
    
    # Create libs directory if not exists
    mkdir -p "${PROJECT_ROOT}/src/libs"
    
    # Upload PyYAML wheel (pre-downloaded, Python 3.10 for Glue 4.0)
    log_info "Uploading PyYAML wheel..."
    local pyyaml_wheel="${PROJECT_ROOT}/src/libs/PyYAML-6.0.1-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"
    if [ ! -f "$pyyaml_wheel" ]; then
        log_error "PyYAML wheel not found: ${pyyaml_wheel}"
        exit 1
    fi
    aws s3 cp "$pyyaml_wheel" "s3://${assets_bucket}/libs/" --region "$REGION"
    
    # Upload PyDeequ wheel (pre-downloaded, REQUIRED for Deequ DQ metrics)
    log_info "Uploading PyDeequ wheel..."
    local pydeequ_wheel="${PROJECT_ROOT}/src/libs/pydeequ-1.2.0-py3-none-any.whl"
    if [ ! -f "$pydeequ_wheel" ]; then
        log_error "PyDeequ wheel not found: ${pydeequ_wheel}"
        exit 1
    fi
    aws s3 cp "$pydeequ_wheel" "s3://${assets_bucket}/libs/" --region "$REGION"
    
    # Upload Deequ JAR (pre-downloaded, REQUIRED for Spark 3.3 / Glue 4.0)
    log_info "Uploading Deequ JAR..."
    local deequ_jar="${PROJECT_ROOT}/src/libs/deequ-2.0.4-spark-3.3.jar"
    if [ ! -f "$deequ_jar" ]; then
        log_error "Deequ JAR not found: ${deequ_jar}"
        exit 1
    fi
    aws s3 cp "$deequ_jar" "s3://${assets_bucket}/libs/" --region "$REGION"
    
    log_success "Assets uploaded (including Deequ libraries)"
}

# =============================================================================
# Start DMS Replication
# =============================================================================
start_dms_replication() {
    log_step "Starting DMS replication task..."
    
    local dms_task_arn
    dms_task_arn=$(get_stack_output "DMSTaskArn")
    
    if [ -z "$dms_task_arn" ]; then
        log_warn "DMS task ARN not found"
        return 0
    fi
    
    local task_status
    task_status=$(aws dms describe-replication-tasks \
        --filters Name=replication-task-arn,Values="$dms_task_arn" \
        --region "$REGION" \
        --query 'ReplicationTasks[0].Status' \
        --output text 2>/dev/null || echo "unknown")
    
    log_info "DMS task status: ${task_status}"
    
    if [ "$task_status" = "stopped" ] || [ "$task_status" = "ready" ]; then
        log_info "Starting DMS task..."
        aws dms start-replication-task \
            --replication-task-arn "$dms_task_arn" \
            --start-replication-task-type start-replication \
            --region "$REGION" > /tmp/dms-output.txt 2>&1 || {
                log_warn "Failed to start DMS task"
            }
    fi
    
    log_success "DMS replication started"
}

# =============================================================================
# Start Glue Job (Optional)
# =============================================================================
start_glue_job() {
    if [ "$START_GLUE" = false ]; then
        log_info "Skipping Glue job start (use --start-glue to enable)"
        return 0
    fi
    
    log_step "Starting Glue streaming job..."
    
    local glue_job_name
    glue_job_name=$(get_stack_output "GlueJobName")
    
    if [ -z "$glue_job_name" ]; then
        log_warn "Glue job name not found"
        return 0
    fi
    
    local running_jobs
    running_jobs=$(aws glue get-job-runs \
        --job-name "$glue_job_name" \
        --region "$REGION" \
        --query "JobRuns[?JobRunState=='RUNNING'].Id" \
        --output text 2>/dev/null || echo "")
    
    if [ -n "$running_jobs" ]; then
        log_info "Glue job already running"
    else
        log_info "Starting Glue job..."
        aws glue start-job-run \
            --job-name "$glue_job_name" \
            --region "$REGION" > /tmp/glue-output.txt 2>&1 || {
                log_warn "Failed to start Glue job"
            }
    fi
    
    log_success "Glue job started"
}

# =============================================================================
# Display Summary
# =============================================================================
display_summary() {
    log_step "Deployment Summary"
    echo ""
    echo "Stack Name: ${STACK_NAME}"
    echo "Region: ${REGION}"
    echo ""
    
    local rds_endpoint delta_bucket assets_bucket glue_job
    rds_endpoint=$(get_stack_output "RDSEndpoint")
    delta_bucket=$(get_stack_output "DeltaBucket")
    assets_bucket=$(get_stack_output "AssetsBucket")
    glue_job=$(get_stack_output "GlueJobName")
    
    [ -n "$rds_endpoint" ] && echo "RDS Endpoint: ${rds_endpoint}"
    [ -n "$delta_bucket" ] && echo "Delta Bucket: ${delta_bucket}"
    [ -n "$assets_bucket" ] && echo "Assets Bucket: ${assets_bucket}"
    [ -n "$glue_job" ] && echo "Glue Job: ${glue_job}"
    echo ""
    
    echo "Next Steps:"
    echo "  1. Create RDS tables: psql -h <RDS_ENDPOINT> -U postgres -f scripts/create_tables.sql"
    echo "  2. Generate test data: aws lambda invoke --function-name ${STACK_NAME}-data-generator ..."
    echo "  3. Query data in Athena"
    echo ""
}

# =============================================================================
# Main
# =============================================================================
main() {
    echo ""
    echo -e "${BOLD}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║     Streaming ETL Framework - One-Click Deployment         ║${NC}"
    echo -e "${BOLD}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    parse_args "$@"
    
    log_info "Stack Name: ${STACK_NAME}"
    log_info "Region: ${REGION}"
    echo ""
    
    check_prerequisites
    echo ""
    
    if [ "$SKIP_STACK" = false ]; then
        deploy_cloudformation_stack
        echo ""
    fi
    
    upload_assets_to_s3
    echo ""
    
    start_dms_replication
    echo ""
    
    start_glue_job
    echo ""
    
    display_summary
    
    log_success "Deployment complete!"
    echo ""
}

main "$@"
