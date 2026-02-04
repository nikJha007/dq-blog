#!/bin/bash
# =============================================================================
# Streaming ETL Framework - Post-Deployment Setup & Simulation
# =============================================================================
# Run this after deploy.sh completes to:
#   1. Create RDS tables
#   2. Create Kafka topics from config
#   3. Start DMS replication
#   4. Start Glue streaming job
#   5. Generate test data
#
# Usage: ./post-deploy.sh [OPTIONS]
#   -s, --stack-name    Stack name (default: dq-etl)
#   -r, --region        AWS region (default: us-east-1)
#   --skip-tables       Skip RDS table creation
#   --skip-topics       Skip Kafka topic creation
#   --skip-dms          Skip starting DMS task
#   --skip-glue         Skip starting Glue job
#   --generate-data     Generate continuous test data
# =============================================================================

set -euo pipefail

# =============================================================================
# Configuration
# =============================================================================
STACK_NAME="${STACK_NAME:-dq-etl}"
REGION="${AWS_REGION:-us-east-1}"
SKIP_TABLES=false
SKIP_TOPICS=false
SKIP_DMS=false
SKIP_GLUE=false
GENERATE_DATA=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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
            --skip-tables) SKIP_TABLES=true; shift ;;
            --skip-topics) SKIP_TOPICS=true; shift ;;
            --skip-dms) SKIP_DMS=true; shift ;;
            --skip-glue) SKIP_GLUE=true; shift ;;
            --generate-data) GENERATE_DATA=true; shift ;;
            *) log_error "Unknown option: $1"; exit 1 ;;
        esac
    done
}

# =============================================================================
# Utility Functions
# =============================================================================
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

get_stack_output() {
    local output_key="$1"
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query "Stacks[0].Outputs[?OutputKey=='${output_key}'].OutputValue" \
        --output text 2>/dev/null
}

# =============================================================================
# Step 1: Create RDS Tables
# =============================================================================
create_rds_tables() {
    if [ "$SKIP_TABLES" = true ]; then
        log_info "Skipping RDS table creation"
        return 0
    fi
    
    log_step "Creating RDS tables..."
    
    local sql_runner="${STACK_NAME}-sql-runner"
    
    # vehicles table
    log_info "Creating vehicles table..."
    invoke_lambda "$sql_runner" '{"sql": "CREATE TABLE IF NOT EXISTS vehicles (id SERIAL PRIMARY KEY, vin VARCHAR(17) NOT NULL UNIQUE, license_plate VARCHAR(20), make VARCHAR(50), model VARCHAR(50), year INTEGER, fuel_type VARCHAR(20) DEFAULT '\''diesel'\'', status VARCHAR(20) DEFAULT '\''active'\'', created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())"}'
    echo ""
    
    # drivers table
    log_info "Creating drivers table..."
    invoke_lambda "$sql_runner" '{"sql": "CREATE TABLE IF NOT EXISTS drivers (id SERIAL PRIMARY KEY, employee_id VARCHAR(20) NOT NULL UNIQUE, name VARCHAR(255) NOT NULL, license_number VARCHAR(50), phone VARCHAR(20), status VARCHAR(20) DEFAULT '\''available'\'', safety_score DECIMAL(3,1) DEFAULT 100.0, created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())"}'
    echo ""
    
    # vehicle_telemetry table
    log_info "Creating vehicle_telemetry table..."
    invoke_lambda "$sql_runner" '{"sql": "CREATE TABLE IF NOT EXISTS vehicle_telemetry (id SERIAL PRIMARY KEY, vehicle_id INTEGER, recorded_at TIMESTAMP NOT NULL DEFAULT NOW(), latitude DECIMAL(10,7), longitude DECIMAL(10,7), speed_kmh DECIMAL(5,1), fuel_level_pct DECIMAL(5,2), engine_temp_c DECIMAL(5,1), odometer_km DECIMAL(10,1), engine_status VARCHAR(20), harsh_braking BOOLEAN DEFAULT FALSE, harsh_acceleration BOOLEAN DEFAULT FALSE, created_at TIMESTAMP DEFAULT NOW())"}'
    echo ""
    
    # deliveries table
    log_info "Creating deliveries table..."
    invoke_lambda "$sql_runner" '{"sql": "CREATE TABLE IF NOT EXISTS deliveries (id SERIAL PRIMARY KEY, vehicle_id INTEGER, driver_id INTEGER, status VARCHAR(20) DEFAULT '\''assigned'\'', pickup_address TEXT, delivery_address TEXT, scheduled_time TIMESTAMP, actual_pickup_time TIMESTAMP, actual_delivery_time TIMESTAMP, customer_name VARCHAR(255), customer_phone VARCHAR(20), notes TEXT, created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())"}'
    echo ""
    
    # alerts table
    log_info "Creating alerts table..."
    invoke_lambda "$sql_runner" '{"sql": "CREATE TABLE IF NOT EXISTS alerts (id SERIAL PRIMARY KEY, vehicle_id INTEGER, driver_id INTEGER, alert_type VARCHAR(50), severity VARCHAR(20), message TEXT, latitude DECIMAL(10,7), longitude DECIMAL(10,7), acknowledged BOOLEAN DEFAULT FALSE, acknowledged_at TIMESTAMP, created_at TIMESTAMP DEFAULT NOW())"}'
    echo ""
    
    log_success "RDS tables created"
}

# =============================================================================
# Step 2: Create Kafka Topics from Config
# =============================================================================
create_kafka_topics() {
    if [ "$SKIP_TOPICS" = true ]; then
        log_info "Skipping Kafka topic creation"
        return 0
    fi
    
    log_step "Creating Kafka topics from config..."
    
    local kafka_admin="${STACK_NAME}-kafka-admin"
    
    log_info "Syncing topics from tables.yaml..."
    local result
    result=$(invoke_lambda "$kafka_admin" '{"action": "sync_from_config"}')
    echo "$result"
    echo ""
    
    log_info "Listing all topics..."
    invoke_lambda "$kafka_admin" '{"action": "list"}'
    echo ""
    
    log_success "Kafka topics created"
}

# =============================================================================
# Step 3: Start DMS Replication Task
# =============================================================================
start_dms_task() {
    if [ "$SKIP_DMS" = true ]; then
        log_info "Skipping DMS task start"
        return 0
    fi
    
    log_step "Starting DMS replication task..."
    
    local dms_task_arn
    dms_task_arn=$(get_stack_output "DMSTaskArn")
    
    if [ -z "$dms_task_arn" ]; then
        log_warn "DMS task ARN not found"
        return 0
    fi
    
    # Get replication instance ARN
    local replication_instance_arn
    replication_instance_arn=$(aws dms describe-replication-instances \
        --filters Name=replication-instance-id,Values="${STACK_NAME}-dms" \
        --region "$REGION" \
        --query 'ReplicationInstances[0].ReplicationInstanceArn' \
        --output text 2>/dev/null || echo "")
    
    # Get source and target endpoint ARNs
    local source_endpoint_arn target_endpoint_arn
    source_endpoint_arn=$(aws dms describe-endpoints \
        --filters Name=endpoint-id,Values="${STACK_NAME}-source-postgres" \
        --region "$REGION" \
        --query 'Endpoints[0].EndpointArn' \
        --output text 2>/dev/null || echo "")
    
    target_endpoint_arn=$(aws dms describe-endpoints \
        --filters Name=endpoint-id,Values="${STACK_NAME}-target-msk" \
        --region "$REGION" \
        --query 'Endpoints[0].EndpointArn' \
        --output text 2>/dev/null || echo "")
    
    # Test connections and wait for them to succeed
    if [ -n "$replication_instance_arn" ] && [ -n "$source_endpoint_arn" ]; then
        log_info "Testing DMS source connection..."
        aws dms test-connection \
            --replication-instance-arn "$replication_instance_arn" \
            --endpoint-arn "$source_endpoint_arn" \
            --region "$REGION" > /dev/null 2>&1 || true
    fi
    
    if [ -n "$replication_instance_arn" ] && [ -n "$target_endpoint_arn" ]; then
        log_info "Testing DMS target connection..."
        aws dms test-connection \
            --replication-instance-arn "$replication_instance_arn" \
            --endpoint-arn "$target_endpoint_arn" \
            --region "$REGION" > /dev/null 2>&1 || true
    fi
    
    # Wait for both connections to be successful (up to 3 minutes)
    log_info "Waiting for DMS connection tests to complete..."
    local max_wait=180
    local elapsed=0
    local interval=15
    local source_status="testing"
    local target_status="testing"
    
    while [ $elapsed -lt $max_wait ]; do
        # Check connection statuses
        source_status=$(aws dms describe-connections \
            --region "$REGION" \
            --query "Connections[?EndpointIdentifier=='${STACK_NAME}-source-postgres'].Status" \
            --output text 2>/dev/null || echo "unknown")
        
        target_status=$(aws dms describe-connections \
            --region "$REGION" \
            --query "Connections[?EndpointIdentifier=='${STACK_NAME}-target-msk'].Status" \
            --output text 2>/dev/null || echo "unknown")
        
        echo -ne "\r  Source: ${source_status}, Target: ${target_status} (${elapsed}s)    "
        
        if [ "$source_status" = "successful" ] && [ "$target_status" = "successful" ]; then
            echo ""
            log_success "Both DMS connections successful"
            break
        fi
        
        if [ "$source_status" = "failed" ] || [ "$target_status" = "failed" ]; then
            echo ""
            log_error "DMS connection test failed - Source: ${source_status}, Target: ${target_status}"
            return 1
        fi
        
        sleep $interval
        elapsed=$((elapsed + interval))
    done
    
    if [ $elapsed -ge $max_wait ]; then
        echo ""
        log_warn "Timeout waiting for connections. Attempting to start anyway..."
    fi
    
    local task_status
    task_status=$(aws dms describe-replication-tasks \
        --filters Name=replication-task-arn,Values="$dms_task_arn" \
        --region "$REGION" \
        --query 'ReplicationTasks[0].Status' \
        --output text 2>/dev/null || echo "unknown")
    
    log_info "Current DMS task status: ${task_status}"
    
    if [ "$task_status" = "stopped" ] || [ "$task_status" = "ready" ]; then
        log_info "Starting DMS task..."
        if aws dms start-replication-task \
            --replication-task-arn "$dms_task_arn" \
            --start-replication-task-type start-replication \
            --region "$REGION" > /tmp/dms-start.json 2>&1; then
            log_success "DMS task started"
        else
            log_error "Failed to start DMS task:"
            cat /tmp/dms-start.json
        fi
    elif [ "$task_status" = "failed" ]; then
        log_warn "DMS task previously failed, restarting with resume-processing..."
        if aws dms start-replication-task \
            --replication-task-arn "$dms_task_arn" \
            --start-replication-task-type resume-processing \
            --region "$REGION" > /tmp/dms-start.json 2>&1; then
            log_success "DMS task resumed"
        else
            log_warn "Resume failed, trying reload-target..."
            if aws dms start-replication-task \
                --replication-task-arn "$dms_task_arn" \
                --start-replication-task-type reload-target \
                --region "$REGION" > /tmp/dms-start.json 2>&1; then
                log_success "DMS task restarted with reload-target"
            else
                log_warn "Reload-target failed, trying start-replication..."
                aws dms start-replication-task \
                    --replication-task-arn "$dms_task_arn" \
                    --start-replication-task-type start-replication \
                    --region "$REGION" > /tmp/dms-start.json 2>&1 || {
                        log_error "All DMS start attempts failed:"
                        cat /tmp/dms-start.json
                    }
            fi
        fi
    elif [ "$task_status" = "running" ]; then
        log_info "DMS task already running"
    elif [ "$task_status" = "starting" ]; then
        log_info "DMS task is starting..."
    else
        log_warn "DMS task in state: ${task_status}, attempting start..."
        aws dms start-replication-task \
            --replication-task-arn "$dms_task_arn" \
            --start-replication-task-type start-replication \
            --region "$REGION" > /tmp/dms-start.json 2>&1 || {
                log_warn "Start failed, trying resume-processing..."
                aws dms start-replication-task \
                    --replication-task-arn "$dms_task_arn" \
                    --start-replication-task-type resume-processing \
                    --region "$REGION" > /tmp/dms-start.json 2>&1 || true
            }
    fi
}

# =============================================================================
# Step 4: Start Glue Streaming Job
# =============================================================================
start_glue_job() {
    if [ "$SKIP_GLUE" = true ]; then
        log_info "Skipping Glue job start"
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
        log_info "Starting Glue job: ${glue_job_name}"
        aws glue start-job-run \
            --job-name "$glue_job_name" \
            --region "$REGION" > /dev/null 2>&1 || {
                log_warn "Failed to start Glue job"
            }
        log_success "Glue job started"
    fi
}

# =============================================================================
# Step 5: Generate Test Data
# =============================================================================
generate_test_data() {
    log_step "Generating test data..."
    
    local sql_runner="${STACK_NAME}-sql-runner"
    
    # Insert sample vehicles
    log_info "Inserting sample vehicles..."
    invoke_lambda "$sql_runner" '{"sql": "INSERT INTO vehicles (vin, license_plate, make, model, year, fuel_type, status) VALUES ('\''1HGBH41JXMN109186'\'', '\''ABC-1234'\'', '\''Honda'\'', '\''Accord'\'', 2023, '\''gasoline'\'', '\''active'\'') ON CONFLICT (vin) DO NOTHING"}'
    invoke_lambda "$sql_runner" '{"sql": "INSERT INTO vehicles (vin, license_plate, make, model, year, fuel_type, status) VALUES ('\''2T1BURHE5JC123456'\'', '\''XYZ-5678'\'', '\''Toyota'\'', '\''Camry'\'', 2022, '\''hybrid'\'', '\''active'\'') ON CONFLICT (vin) DO NOTHING"}'
    invoke_lambda "$sql_runner" '{"sql": "INSERT INTO vehicles (vin, license_plate, make, model, year, fuel_type, status) VALUES ('\''3VWDX7AJ5DM123789'\'', '\''DEF-9012'\'', '\''Volkswagen'\'', '\''Jetta'\'', 2021, '\''diesel'\'', '\''active'\'') ON CONFLICT (vin) DO NOTHING"}'
    echo ""
    
    # Insert sample drivers
    log_info "Inserting sample drivers..."
    invoke_lambda "$sql_runner" '{"sql": "INSERT INTO drivers (employee_id, name, license_number, phone, status, safety_score) VALUES ('\''EMP001'\'', '\''John Smith'\'', '\''DL123456'\'', '\''555-0101'\'', '\''available'\'', 95.5) ON CONFLICT (employee_id) DO NOTHING"}'
    invoke_lambda "$sql_runner" '{"sql": "INSERT INTO drivers (employee_id, name, license_number, phone, status, safety_score) VALUES ('\''EMP002'\'', '\''Jane Doe'\'', '\''DL789012'\'', '\''555-0102'\'', '\''on_duty'\'', 98.0) ON CONFLICT (employee_id) DO NOTHING"}'
    echo ""
    
    # Insert sample telemetry (this will trigger DQ rules)
    log_info "Inserting sample telemetry data..."
    invoke_lambda "$sql_runner" '{"sql": "INSERT INTO vehicle_telemetry (vehicle_id, latitude, longitude, speed_kmh, fuel_level_pct, engine_temp_c, odometer_km, engine_status) VALUES (1, 37.7749, -122.4194, 65.5, 75.0, 90.0, 45000.0, '\''running'\'')"}'
    invoke_lambda "$sql_runner" '{"sql": "INSERT INTO vehicle_telemetry (vehicle_id, latitude, longitude, speed_kmh, fuel_level_pct, engine_temp_c, odometer_km, engine_status) VALUES (2, 34.0522, -118.2437, 80.0, 50.0, 85.0, 32000.0, '\''running'\'')"}'
    # Insert bad data to test DQ quarantine (speed > 350)
    invoke_lambda "$sql_runner" '{"sql": "INSERT INTO vehicle_telemetry (vehicle_id, latitude, longitude, speed_kmh, fuel_level_pct, engine_temp_c, odometer_km, engine_status) VALUES (3, 40.7128, -74.0060, 400.0, 25.0, 95.0, 28000.0, '\''running'\'')"}'
    echo ""
    
    # Insert sample deliveries
    log_info "Inserting sample deliveries..."
    invoke_lambda "$sql_runner" '{"sql": "INSERT INTO deliveries (vehicle_id, driver_id, status, pickup_address, delivery_address, customer_name, customer_phone) VALUES (1, 1, '\''assigned'\'', '\''123 Main St'\'', '\''456 Oak Ave'\'', '\''Alice Johnson'\'', '\''555-1234'\'')"}'
    invoke_lambda "$sql_runner" '{"sql": "INSERT INTO deliveries (vehicle_id, driver_id, status, pickup_address, delivery_address, customer_name, customer_phone) VALUES (2, 2, '\''in_transit'\'', '\''789 Pine Rd'\'', '\''321 Elm St'\'', '\''Bob Williams'\'', '\''555-5678'\'')"}'
    echo ""
    
    # Insert sample alerts
    log_info "Inserting sample alerts..."
    invoke_lambda "$sql_runner" '{"sql": "INSERT INTO alerts (vehicle_id, driver_id, alert_type, severity, message, latitude, longitude) VALUES (1, 1, '\''speeding'\'', '\''warning'\'', '\''Vehicle exceeded speed limit'\'', 37.7749, -122.4194)"}'
    invoke_lambda "$sql_runner" '{"sql": "INSERT INTO alerts (vehicle_id, driver_id, alert_type, severity, message, latitude, longitude) VALUES (3, NULL, '\''low_fuel'\'', '\''info'\'', '\''Fuel level below 30%'\'', 40.7128, -74.0060)"}'
    echo ""
    
    log_success "Test data generated"
}

# =============================================================================
# Step 6: Continuous Data Generation (Optional)
# =============================================================================
generate_continuous_data() {
    if [ "$GENERATE_DATA" = false ]; then
        return 0
    fi
    
    log_step "Generating continuous test data (Ctrl+C to stop)..."
    
    local sql_runner="${STACK_NAME}-sql-runner"
    local counter=1
    
    while true; do
        local lat=$(echo "scale=4; 30 + ($RANDOM % 200) / 10" | bc)
        local lon=$(echo "scale=4; -120 + ($RANDOM % 500) / 10" | bc)
        local speed=$(echo "scale=1; ($RANDOM % 120)" | bc)
        local fuel=$(echo "scale=1; 20 + ($RANDOM % 80)" | bc)
        local vehicle_id=$(( (RANDOM % 3) + 1 ))
        
        log_info "Inserting telemetry record #${counter}..."
        invoke_lambda "$sql_runner" "{\"sql\": \"INSERT INTO vehicle_telemetry (vehicle_id, latitude, longitude, speed_kmh, fuel_level_pct, engine_temp_c, odometer_km, engine_status) VALUES (${vehicle_id}, ${lat}, ${lon}, ${speed}, ${fuel}, 85.0, 50000.0, 'running')\"}" > /dev/null
        
        counter=$((counter + 1))
        sleep 5
    done
}

# =============================================================================
# Step 7: Create Athena Tables via Lambda
# =============================================================================
create_athena_tables() {
    log_step "Creating Athena tables for Delta Lake..."
    
    local athena_creator="${STACK_NAME}-athena-table-creator"
    
    log_info "Invoking Athena table creator Lambda..."
    local result
    result=$(invoke_lambda "$athena_creator" '{"action": "create_all"}')
    echo "$result"
    echo ""
    
    log_success "Athena tables created"
    
    local database="${STACK_NAME//-/_}_db"
    log_info "Query tables in Athena: SELECT * FROM ${database}.vehicles LIMIT 10;"
}

# =============================================================================
# Display Summary
# =============================================================================
display_summary() {
    log_step "Post-Deployment Summary"
    echo ""
    
    local rds_endpoint delta_bucket glue_job
    rds_endpoint=$(get_stack_output "RDSEndpoint")
    delta_bucket=$(get_stack_output "DeltaBucket")
    glue_job=$(get_stack_output "GlueJobName")
    
    echo "Stack Name: ${STACK_NAME}"
    echo "Region: ${REGION}"
    echo ""
    [ -n "$rds_endpoint" ] && echo "RDS Endpoint: ${rds_endpoint}"
    [ -n "$delta_bucket" ] && echo "Delta Bucket: ${delta_bucket}"
    [ -n "$glue_job" ] && echo "Glue Job: ${glue_job}"
    echo ""
    
    echo "Next Steps:"
    echo "  1. Check Glue job status in AWS Console"
    echo "  2. Query Delta Lake tables in Athena:"
    echo "     SELECT * FROM ${STACK_NAME}_db.vehicles;"
    echo "     SELECT * FROM ${STACK_NAME}_db.vehicle_telemetry;"
    echo "  3. Check quarantine bucket for DQ failures:"
    echo "     aws s3 ls s3://${STACK_NAME}-quarantine-\${AWS_ACCOUNT_ID}/ --recursive"
    echo "  4. Generate more data:"
    echo "     ./post-deploy.sh --stack-name ${STACK_NAME} --skip-tables --skip-topics --skip-dms --skip-glue --generate-data"
    echo ""
}

# =============================================================================
# Main
# =============================================================================
main() {
    echo ""
    echo -e "${BOLD}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║   Streaming ETL Framework - Post-Deployment Setup          ║${NC}"
    echo -e "${BOLD}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    parse_args "$@"
    
    log_info "Stack Name: ${STACK_NAME}"
    log_info "Region: ${REGION}"
    echo ""
    
    create_rds_tables
    echo ""
    
    create_kafka_topics
    echo ""
    
    start_dms_task
    echo ""
    
    start_glue_job
    echo ""
    
    generate_test_data
    echo ""
    
    display_summary
    
    create_athena_tables
    echo ""
    
    generate_continuous_data
    
    log_success "Post-deployment setup complete!"
    echo ""
}

main "$@"
