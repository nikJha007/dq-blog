#!/bin/bash
# =============================================================================
# Streaming ETL Framework - Teardown Script
# =============================================================================
# Complete cleanup of all resources created by the stack.
# Continues on errors to ensure maximum cleanup.
#
# Usage: ./teardown.sh [OPTIONS]
#   -s, --stack-name    Stack name (default: streaming-etl)
#   -r, --region        AWS region (default: us-east-1)
#   -f, --force         Skip confirmation prompt
#   -h, --help          Show this help message
# =============================================================================

set +e

STACK_NAME="${STACK_NAME:-streaming-etl}"
REGION="${AWS_REGION:-us-east-1}"
FORCE=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

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
    log_step "Stopping Glue streaming jobs..."
    local glue_job="${STACK_NAME}-streaming-job"
    
    if ! aws glue get-job --job-name "$glue_job" --region "$REGION" &>/dev/null; then
        log_info "Glue job '${glue_job}' does not exist, skipping"
        return 0
    fi
    
    local running_jobs
    running_jobs=$(aws glue get-job-runs --job-name "$glue_job" --region "$REGION" \
        --query "JobRuns[?JobRunState=='RUNNING' || JobRunState=='STARTING'].JobRunId" \
        --output text 2>/dev/null || echo "")
    
    if [ -z "$running_jobs" ] || [ "$running_jobs" == "None" ]; then
        log_info "No running Glue job runs found"
        return 0
    fi
    
    log_info "Stopping running Glue jobs..."
    for job_run in $running_jobs; do
        aws glue batch-stop-job-run --job-name "$glue_job" --job-run-ids "$job_run" --region "$REGION" 2>/dev/null || {
            log_warn "Failed to stop Glue job run: ${job_run}"
        }
    done
    
    log_info "Waiting 30s for Glue jobs to stop..."
    sleep 30
    log_success "Glue jobs stopped"
}

stop_dms_task() {
    log_step "Stopping DMS replication task..."
    
    local dms_task_arn
    dms_task_arn=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
        --query "Stacks[0].Outputs[?OutputKey=='DMSTaskArn'].OutputValue" --output text 2>/dev/null || echo "")
    
    if [ -z "$dms_task_arn" ] || [ "$dms_task_arn" == "None" ]; then
        log_info "DMS task ARN not found, skipping"
        return 0
    fi
    
    local dms_status
    dms_status=$(aws dms describe-replication-tasks \
        --filters Name=replication-task-arn,Values="$dms_task_arn" \
        --region "$REGION" \
        --query "ReplicationTasks[0].Status" --output text 2>/dev/null || echo "")
    
    if [ "$dms_status" == "running" ] || [ "$dms_status" == "starting" ]; then
        log_info "Stopping DMS replication task (status: ${dms_status})..."
        aws dms stop-replication-task --replication-task-arn "$dms_task_arn" --region "$REGION" 2>/dev/null || {
            log_warn "Failed to stop DMS task"
        }
        log_info "Waiting 60s for DMS task to stop..."
        sleep 60
    else
        log_info "DMS task status: ${dms_status:-unknown}"
    fi
    
    log_success "DMS task stopped"
}

empty_bucket() {
    local bucket=$1
    if [ -z "$bucket" ] || [ "$bucket" == "None" ]; then
        return 0
    fi
    
    if ! aws s3api head-bucket --bucket "$bucket" --region "$REGION" 2>/dev/null; then
        log_info "Bucket '${bucket}' does not exist, skipping"
        return 0
    fi
    
    log_info "Emptying bucket: ${bucket}"
    aws s3 rm "s3://${bucket}" --recursive --region "$REGION" 2>/dev/null || {
        log_warn "Failed to empty bucket: ${bucket}"
    }
    
    # Delete versions for versioned buckets
    local versions
    versions=$(aws s3api list-object-versions --bucket "$bucket" --region "$REGION" \
        --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' --output json 2>/dev/null || echo '{"Objects":null}')
    
    if [ "$versions" != '{"Objects":null}' ] && [ "$(echo "$versions" | python3.10 -c 'import sys,json; d=json.load(sys.stdin); print(len(d.get("Objects") or []))' 2>/dev/null || echo 0)" -gt 0 ]; then
        echo "$versions" | aws s3api delete-objects --bucket "$bucket" --delete file:///dev/stdin --region "$REGION" 2>/dev/null || {
            log_warn "Failed to delete object versions in: ${bucket}"
        }
    fi
}

empty_buckets() {
    log_step "Emptying S3 buckets..."
    
    local buckets
    buckets=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
        --query "Stacks[0].Outputs[?contains(OutputKey, 'Bucket')].OutputValue" --output text 2>/dev/null || echo "")
    
    if [ -z "$buckets" ] || [ "$buckets" == "None" ]; then
        log_info "No buckets found in stack outputs"
        return 0
    fi
    
    for bucket in $buckets; do
        empty_bucket "$bucket"
    done
    
    log_success "S3 buckets emptied"
}

delete_lambda_layers() {
    log_step "Cleaning up Lambda layers..."
    
    local layer_names=(
        "${STACK_NAME}-psycopg2"
        "${STACK_NAME}-kafka-python"
        "${STACK_NAME}-pyyaml"
    )
    
    for layer_name in "${layer_names[@]}"; do
        local versions
        versions=$(aws lambda list-layer-versions \
            --layer-name "$layer_name" \
            --region "$REGION" \
            --query 'LayerVersions[].Version' \
            --output text 2>/dev/null || echo "")
        
        if [ -n "$versions" ] && [ "$versions" != "None" ]; then
            for version in $versions; do
                log_info "Deleting layer ${layer_name} version ${version}..."
                aws lambda delete-layer-version \
                    --layer-name "$layer_name" \
                    --version-number "$version" \
                    --region "$REGION" 2>/dev/null || {
                        log_warn "Failed to delete layer ${layer_name} v${version}"
                    }
            done
        fi
    done
    
    log_success "Lambda layers cleaned up"
}

delete_stack() {
    log_step "Deleting CloudFormation stack: ${STACK_NAME}"
    
    if ! check_stack_exists; then
        log_info "Stack '${STACK_NAME}' does not exist"
        return 0
    fi
    
    aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$REGION" 2>/dev/null || {
        log_error "Failed to initiate stack deletion"
        return 1
    }
    
    log_info "Waiting for stack deletion (this may take 15-30 minutes)..."
    aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" --region "$REGION" 2>/dev/null || {
        log_warn "Stack deletion may still be in progress or failed"
        log_warn "Check AWS Console for status"
    }
    
    log_success "Stack deleted"
}

cleanup_local() {
    log_step "Cleaning up local build artifacts..."
    
    if [ -d "${PROJECT_ROOT}/build" ]; then
        rm -rf "${PROJECT_ROOT}/build"
        log_info "Removed build/ directory"
    fi
    
    log_success "Local cleanup complete"
}

main() {
    parse_args "$@"
    
    echo ""
    echo -e "${BOLD}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║     Streaming ETL Framework - Stack Teardown               ║${NC}"
    echo -e "${BOLD}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    log_warn "This will DELETE all resources in stack: ${STACK_NAME}"
    log_warn "Region: ${REGION}"
    echo ""
    
    if [ "$FORCE" != true ]; then
        read -p "Are you sure? (yes/no): " confirm
        if [ "$confirm" != "yes" ]; then
            log_info "Teardown cancelled"
            exit 0
        fi
    fi
    
    echo ""
    
    stop_glue_jobs
    echo ""
    
    stop_dms_task
    echo ""
    
    empty_buckets
    echo ""
    
    delete_lambda_layers
    echo ""
    
    delete_stack
    echo ""
    
    cleanup_local
    echo ""
    
    log_success "Teardown complete for stack: ${STACK_NAME}"
    echo ""
}

main "$@"
