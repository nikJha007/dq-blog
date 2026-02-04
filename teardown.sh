#!/bin/bash
# Streaming ETL Framework - Teardown Script
# Complete cleanup of all resources created by the stack

set +e

STACK_NAME="${STACK_NAME:-streaming-etl}"
REGION="${AWS_REGION:-us-east-1}"
FORCE=false

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --force|-f) FORCE=true; shift ;;
            --stack-name|-s) STACK_NAME="$2"; shift 2 ;;
            --region|-r) REGION="$2"; shift 2 ;;
            --help|-h) show_help; exit 0 ;;
            *) log_error "Unknown option: $1"; show_help; exit 1 ;;
        esac
    done
}

show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo "  --force, -f           Skip confirmation prompt"
    echo "  --stack-name, -s      CloudFormation stack name (default: streaming-etl)"
    echo "  --region, -r          AWS region (default: us-east-1)"
    echo "  --help, -h            Show this help message"
}

check_stack_exists() {
    aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" &>/dev/null
    return $?
}


stop_glue_jobs() {
    log_info "Checking for running Glue jobs..."
    GLUE_JOB="${STACK_NAME}-streaming-job"
    
    if ! aws glue get-job --job-name "$GLUE_JOB" --region "$REGION" &>/dev/null; then
        log_warn "Glue job '$GLUE_JOB' does not exist, skipping..."
        return 0
    fi
    
    RUNNING_JOBS=$(aws glue get-job-runs --job-name "$GLUE_JOB" --region "$REGION" \
        --query "JobRuns[?JobRunState=='RUNNING' || JobRunState=='STARTING'].JobRunId" \
        --output text 2>/dev/null || echo "")
    
    if [ -z "$RUNNING_JOBS" ] || [ "$RUNNING_JOBS" == "None" ]; then
        log_info "No running Glue job runs found"
        return 0
    fi
    
    log_info "Stopping running Glue jobs..."
    for job_run in $RUNNING_JOBS; do
        aws glue batch-stop-job-run --job-name "$GLUE_JOB" --job-run-ids "$job_run" --region "$REGION" 2>/dev/null || true
    done
    
    log_info "Waiting for Glue jobs to stop..."
    sleep 30
}

stop_dms_task() {
    log_info "Checking for running DMS replication tasks..."
    
    DMS_TASK_ARN=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
        --query "Stacks[0].Outputs[?OutputKey=='DMSTaskArn'].OutputValue" --output text 2>/dev/null || echo "")
    
    if [ -z "$DMS_TASK_ARN" ] || [ "$DMS_TASK_ARN" == "None" ]; then
        log_warn "DMS task ARN not found, skipping..."
        return 0
    fi
    
    DMS_STATUS=$(aws dms describe-replication-tasks \
        --filters Name=replication-task-arn,Values="$DMS_TASK_ARN" \
        --region "$REGION" \
        --query "ReplicationTasks[0].Status" --output text 2>/dev/null || echo "")
    
    if [ "$DMS_STATUS" == "running" ] || [ "$DMS_STATUS" == "starting" ]; then
        log_info "Stopping DMS replication task..."
        aws dms stop-replication-task --replication-task-arn "$DMS_TASK_ARN" --region "$REGION" 2>/dev/null || true
        sleep 60
    fi
}


empty_bucket() {
    local bucket=$1
    if [ -z "$bucket" ] || [ "$bucket" == "None" ]; then
        return 0
    fi
    
    if ! aws s3api head-bucket --bucket "$bucket" --region "$REGION" 2>/dev/null; then
        log_warn "Bucket '$bucket' does not exist, skipping..."
        return 0
    fi
    
    log_info "Emptying bucket: $bucket"
    aws s3 rm "s3://${bucket}" --recursive --region "$REGION" 2>/dev/null || true
    
    # Delete versions for versioned buckets
    local versions
    versions=$(aws s3api list-object-versions --bucket "$bucket" --region "$REGION" \
        --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' --output json 2>/dev/null || echo '{"Objects":null}')
    
    if [ "$versions" != '{"Objects":null}' ] && [ "$(echo "$versions" | jq '.Objects | length')" -gt 0 ]; then
        echo "$versions" | aws s3api delete-objects --bucket "$bucket" --delete file:///dev/stdin --region "$REGION" 2>/dev/null || true
    fi
}

empty_buckets() {
    log_info "Emptying S3 buckets..."
    
    BUCKETS=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
        --query "Stacks[0].Outputs[?contains(OutputKey, 'Bucket')].OutputValue" --output text 2>/dev/null || echo "")
    
    for bucket in $BUCKETS; do
        empty_bucket "$bucket"
    done
}

delete_stack() {
    log_info "Deleting CloudFormation stack: $STACK_NAME"
    
    if ! check_stack_exists; then
        log_warn "Stack '$STACK_NAME' does not exist"
        return 0
    fi
    
    aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$REGION" 2>/dev/null || {
        log_error "Failed to delete stack"
        return 1
    }
    
    log_info "Waiting for stack deletion (this may take 15-30 minutes)..."
    aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" --region "$REGION" 2>/dev/null || {
        log_warn "Stack deletion may still be in progress"
    }
}

main() {
    parse_args "$@"
    
    echo ""
    log_warn "=========================================="
    log_warn "  STREAMING ETL FRAMEWORK - TEARDOWN"
    log_warn "=========================================="
    log_warn "This will DELETE all resources in stack: $STACK_NAME"
    log_warn "Region: $REGION"
    echo ""
    
    if [ "$FORCE" != true ]; then
        read -p "Are you sure? (yes/no): " confirm
        if [ "$confirm" != "yes" ]; then
            log_info "Teardown cancelled"
            exit 0
        fi
    fi
    
    stop_glue_jobs
    stop_dms_task
    empty_buckets
    delete_stack
    
    log_info "Teardown complete!"
}

main "$@"
