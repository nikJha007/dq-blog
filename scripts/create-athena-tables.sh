#!/bin/bash
# =============================================================================
# Create Athena Tables for Delta Lake
# =============================================================================
# Run this AFTER data has been written to Delta Lake by the Glue streaming job.
# The script checks if Delta data exists before creating tables.
#
# Usage: ./create-athena-tables.sh [OPTIONS]
#   -s, --stack-name    Stack name (default: dq-etl)
#   -r, --region        AWS region (default: us-east-1)
#   --force             Skip data existence check
# =============================================================================

set -euo pipefail

# =============================================================================
# Configuration
# =============================================================================
STACK_NAME="${STACK_NAME:-dq-etl}"
REGION="${AWS_REGION:-us-east-1}"
FORCE=false

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
# Parse Arguments
# =============================================================================
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -s|--stack-name) STACK_NAME="$2"; shift 2 ;;
            -r|--region) REGION="$2"; shift 2 ;;
            --force) FORCE=true; shift ;;
            *) log_error "Unknown option: $1"; exit 1 ;;
        esac
    done
}

# =============================================================================
# Utility Functions
# =============================================================================
get_stack_output() {
    local output_key="$1"
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query "Stacks[0].Outputs[?OutputKey=='${output_key}'].OutputValue" \
        --output text 2>/dev/null
}

invoke_lambda() {
    local function_name="$1"
    local payload="$2"
    local output_file="/tmp/lambda-response.json"
    
    aws lambda invoke \
        --function-name "$function_name" \
        --payload "$payload" \
        --cli-binary-format raw-in-base64-out \
        --region "$REGION" \
        "$output_file" > /dev/null 2>&1
    
    cat "$output_file"
}

# =============================================================================
# Check Delta Data Exists
# =============================================================================
check_delta_data() {
    local delta_bucket="$1"
    local tables=("vehicles" "drivers" "vehicle_telemetry" "deliveries" "alerts")
    local has_data=false
    
    log_step "Checking for Delta Lake data in S3..."
    
    for table in "${tables[@]}"; do
        local delta_log="s3://${delta_bucket}/${table}/_delta_log/"
        local count
        count=$(aws s3 ls "$delta_log" --region "$REGION" 2>/dev/null | wc -l || echo "0")
        
        if [ "$count" -gt 0 ]; then
            log_info "  ✓ ${table}: Delta log found (${count} files)"
            has_data=true
        else
            log_warn "  ✗ ${table}: No Delta log found"
        fi
    done
    
    echo ""
    
    if [ "$has_data" = false ]; then
        return 1
    fi
    return 0
}

# =============================================================================
# Create Athena Tables
# =============================================================================
create_athena_tables() {
    log_step "Creating Athena tables for Delta Lake..."
    
    local athena_creator="${STACK_NAME}-athena-table-creator"
    
    log_info "Invoking Athena table creator Lambda..."
    local result
    result=$(invoke_lambda "$athena_creator" '{"action": "create_all"}')
    
    # Parse result
    local created failed
    created=$(echo "$result" | grep -o '"created": \[[^]]*\]' | sed 's/"created": \[//' | sed 's/\]//' | tr -d '"' | tr ',' ' ')
    failed=$(echo "$result" | grep -o '"failed": \[[^]]*\]' || echo "")
    
    echo ""
    if [ -n "$created" ] && [ "$created" != " " ]; then
        for table in $created; do
            log_success "  ✓ Created table: ${table}"
        done
    fi
    
    if echo "$result" | grep -q '"failed": \[\]'; then
        log_success "All tables created successfully!"
    else
        log_warn "Some tables may have failed. Full response:"
        echo "$result" | python3 -m json.tool 2>/dev/null || echo "$result"
    fi
    
    echo ""
    local database="${STACK_NAME//-/_}_db"
    log_info "Query tables in Athena:"
    echo "  SELECT * FROM \"${STACK_NAME}_db\".\"vehicles\" LIMIT 10;"
    echo "  SELECT * FROM \"${STACK_NAME}_db\".\"vehicle_telemetry\" LIMIT 10;"
}

# =============================================================================
# Generate Test Data
# =============================================================================
prompt_generate_data() {
    echo ""
    log_warn "No Delta data found. The Glue streaming job may not have processed any data yet."
    echo ""
    echo "Options:"
    echo "  1. Generate test data and wait for Glue to process it"
    echo "  2. Exit and run this script later"
    echo ""
    read -p "Generate test data now? (y/n): " choice
    
    if [[ "$choice" =~ ^[Yy]$ ]]; then
        generate_test_data
        echo ""
        log_info "Waiting 60 seconds for Glue to process data..."
        sleep 60
        return 0
    else
        log_info "Run this script again after data has been processed by Glue."
        exit 0
    fi
}

generate_test_data() {
    log_step "Generating test data..."
    
    local sql_runner="${STACK_NAME}-sql-runner"
    
    # Insert sample vehicles
    log_info "Inserting sample vehicles..."
    invoke_lambda "$sql_runner" '{"sql": "INSERT INTO vehicles (vin, license_plate, make, model, year, fuel_type, status) VALUES ('\''1HGBH41JXMN109186'\'', '\''ABC-1234'\'', '\''Honda'\'', '\''Accord'\'', 2023, '\''gasoline'\'', '\''active'\'') ON CONFLICT (vin) DO NOTHING"}' > /dev/null
    invoke_lambda "$sql_runner" '{"sql": "INSERT INTO vehicles (vin, license_plate, make, model, year, fuel_type, status) VALUES ('\''2T1BURHE5JC123456'\'', '\''XYZ-5678'\'', '\''Toyota'\'', '\''Camry'\'', 2022, '\''hybrid'\'', '\''active'\'') ON CONFLICT (vin) DO NOTHING"}' > /dev/null
    
    # Insert sample drivers
    log_info "Inserting sample drivers..."
    invoke_lambda "$sql_runner" '{"sql": "INSERT INTO drivers (employee_id, name, license_number, phone, status, safety_score) VALUES ('\''EMP001'\'', '\''John Smith'\'', '\''DL123456'\'', '\''555-0101'\'', '\''available'\'', 95.5) ON CONFLICT (employee_id) DO NOTHING"}' > /dev/null
    
    # Insert sample telemetry
    log_info "Inserting sample telemetry..."
    invoke_lambda "$sql_runner" '{"sql": "INSERT INTO vehicle_telemetry (vehicle_id, latitude, longitude, speed_kmh, fuel_level_pct, engine_temp_c, odometer_km, engine_status) VALUES (1, 37.7749, -122.4194, 65.5, 75.0, 90.0, 45000.0, '\''running'\'')"}' > /dev/null
    
    # Insert sample deliveries
    log_info "Inserting sample deliveries..."
    invoke_lambda "$sql_runner" '{"sql": "INSERT INTO deliveries (vehicle_id, driver_id, status, pickup_address, delivery_address, customer_name, customer_phone) VALUES (1, 1, '\''assigned'\'', '\''123 Main St'\'', '\''456 Oak Ave'\'', '\''Alice Johnson'\'', '\''555-1234'\'')"}' > /dev/null
    
    # Insert sample alerts
    log_info "Inserting sample alerts..."
    invoke_lambda "$sql_runner" '{"sql": "INSERT INTO alerts (vehicle_id, driver_id, alert_type, severity, message, latitude, longitude) VALUES (1, 1, '\''speeding'\'', '\''warning'\'', '\''Vehicle exceeded speed limit'\'', 37.7749, -122.4194)"}' > /dev/null
    
    log_success "Test data generated"
}

# =============================================================================
# Main
# =============================================================================
main() {
    echo ""
    echo -e "${BOLD}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║   Create Athena Tables for Delta Lake                      ║${NC}"
    echo -e "${BOLD}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    parse_args "$@"
    
    log_info "Stack Name: ${STACK_NAME}"
    log_info "Region: ${REGION}"
    echo ""
    
    # Get Delta bucket
    local delta_bucket
    delta_bucket=$(get_stack_output "DeltaBucket")
    
    if [ -z "$delta_bucket" ]; then
        log_error "Could not find Delta bucket. Is the stack deployed?"
        exit 1
    fi
    
    log_info "Delta Bucket: ${delta_bucket}"
    echo ""
    
    # Check if Delta data exists
    if [ "$FORCE" = false ]; then
        if ! check_delta_data "$delta_bucket"; then
            prompt_generate_data
            # Re-check after data generation
            if ! check_delta_data "$delta_bucket"; then
                log_warn "Still no Delta data found. Glue may need more time to process."
                log_info "You can run with --force to create tables anyway."
                exit 1
            fi
        fi
    else
        log_info "Skipping data check (--force)"
    fi
    
    echo ""
    create_athena_tables
    
    echo ""
    log_success "Athena table creation complete!"
    echo ""
}

main "$@"
