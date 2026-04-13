-- ============================================================================
-- IoT Fleet Management - Analytics Queries
-- Database: streaming_etl_db (Athena/Delta Lake)
-- ============================================================================

-- ============================================================================
-- 1. RECORD COUNTS - Overview of all tables
-- ============================================================================
SELECT 'vehicles' as table_name, COUNT(*) as record_count FROM streaming_etl_db.vehicles
UNION ALL SELECT 'drivers', COUNT(*) FROM streaming_etl_db.drivers
UNION ALL SELECT 'vehicle_telemetry', COUNT(*) FROM streaming_etl_db.vehicle_telemetry
UNION ALL SELECT 'deliveries', COUNT(*) FROM streaming_etl_db.deliveries
UNION ALL SELECT 'alerts', COUNT(*) FROM streaming_etl_db.alerts;

-- ============================================================================
-- 2. SCD TYPE 2 IN ACTION - View historical changes
-- ============================================================================

-- 2a. Current vs Historical records per table
SELECT 'vehicles' as table_name,
       SUM(CASE WHEN _is_current = true THEN 1 ELSE 0 END) as current_records,
       SUM(CASE WHEN _is_current = false THEN 1 ELSE 0 END) as historical_records
FROM streaming_etl_db.vehicles
UNION ALL
SELECT 'drivers',
       SUM(CASE WHEN _is_current = true THEN 1 ELSE 0 END),
       SUM(CASE WHEN _is_current = false THEN 1 ELSE 0 END)
FROM streaming_etl_db.drivers
UNION ALL
SELECT 'deliveries',
       SUM(CASE WHEN _is_current = true THEN 1 ELSE 0 END),
       SUM(CASE WHEN _is_current = false THEN 1 ELSE 0 END)
FROM streaming_etl_db.deliveries;

-- 2b. Delivery status history (SCD2 timeline)
SELECT id, status, _operation, _effective_from, _effective_to, _is_current
FROM streaming_etl_db.deliveries
ORDER BY id, _effective_from;


-- ============================================================================
-- 3. DATA QUALITY (DQ) VALIDATION RESULTS
-- ============================================================================

-- 3a. Telemetry data within valid ranges (DQ passed)
SELECT COUNT(*) as valid_telemetry,
       AVG(speed_kmh) as avg_speed,
       AVG(fuel_level_pct) as avg_fuel,
       MIN(latitude) as min_lat, MAX(latitude) as max_lat,
       MIN(longitude) as min_lon, MAX(longitude) as max_lon
FROM streaming_etl_db.vehicle_telemetry
WHERE latitude BETWEEN -90 AND 90
  AND longitude BETWEEN -180 AND 180
  AND speed_kmh BETWEEN 0 AND 350
  AND fuel_level_pct BETWEEN 0 AND 120;

-- 3b. Check for any DQ edge cases
SELECT 
    SUM(CASE WHEN latitude < -90 OR latitude > 90 THEN 1 ELSE 0 END) as invalid_lat,
    SUM(CASE WHEN longitude < -180 OR longitude > 180 THEN 1 ELSE 0 END) as invalid_lon,
    SUM(CASE WHEN speed_kmh < 0 OR speed_kmh > 350 THEN 1 ELSE 0 END) as invalid_speed,
    SUM(CASE WHEN fuel_level_pct < 0 OR fuel_level_pct > 120 THEN 1 ELSE 0 END) as invalid_fuel
FROM streaming_etl_db.vehicle_telemetry;

-- ============================================================================
-- 4. DATA TRANSFORMS (DT) IN ACTION
-- ============================================================================

-- 4a. PII Masking on drivers (phone and license_number masked)
SELECT id, name, phone, license_number, status
FROM streaming_etl_db.drivers
LIMIT 10;

-- 4b. Coordinate rounding on telemetry (5 decimal places)
SELECT id, vehicle_id, latitude, longitude
FROM streaming_etl_db.vehicle_telemetry
LIMIT 10;

-- ============================================================================
-- 5. FLEET ANALYTICS
-- ============================================================================

-- 5a. Vehicle telemetry summary by vehicle
SELECT vehicle_id,
       COUNT(*) as readings,
       ROUND(AVG(speed_kmh), 2) as avg_speed,
       ROUND(MAX(speed_kmh), 2) as max_speed,
       ROUND(AVG(fuel_level_pct), 2) as avg_fuel
FROM streaming_etl_db.vehicle_telemetry
GROUP BY vehicle_id
ORDER BY vehicle_id;

-- 5b. Alert summary by type and severity
SELECT alert_type, severity, COUNT(*) as count
FROM streaming_etl_db.alerts
GROUP BY alert_type, severity
ORDER BY count DESC;

-- 5c. Delivery performance
SELECT status,
       COUNT(*) as count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM streaming_etl_db.deliveries
WHERE _is_current = true
GROUP BY status;

-- ============================================================================
-- 6. INGESTION METRICS
-- ============================================================================

-- 6a. Records ingested over time (by hour)
SELECT DATE_TRUNC('hour', _ingested_at) as hour,
       COUNT(*) as records_ingested
FROM streaming_etl_db.vehicle_telemetry
GROUP BY DATE_TRUNC('hour', _ingested_at)
ORDER BY hour;

-- 6b. Operation breakdown (INSERT/UPDATE/DELETE)
SELECT _operation, COUNT(*) as count
FROM streaming_etl_db.vehicle_telemetry
GROUP BY _operation;

-- ============================================================================
-- 7. DQ METRICS TABLE (Deequ)
-- ============================================================================

-- Query DQ metrics trends
SELECT 
    table_name,
    metric_name,
    column_name,
    AVG(metric_value) as avg_value,
    MIN(metric_value) as min_value,
    MAX(metric_value) as max_value,
    SUM(CASE WHEN passed = false THEN 1 ELSE 0 END) as failures
FROM streaming_etl_db.dq_metrics
WHERE timestamp > current_timestamp - interval '24' hour
GROUP BY table_name, metric_name, column_name;
