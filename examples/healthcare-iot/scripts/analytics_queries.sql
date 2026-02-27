-- ============================================================================
-- Healthcare IoT - Sample Athena Analytics Queries
-- ============================================================================
-- Run these in the Athena console after deploying the healthcare-iot use case.
-- Database: {stack_name}_db (e.g., health_demo_db)
-- ============================================================================

-- 1. Current patient roster (SCD2 current records, PII masked)
SELECT id, mrn, name, status, _effective_from
FROM patients
WHERE _is_current = true
ORDER BY id;

-- 2. Latest vitals per patient
SELECT p.mrn, p.name,
       v.heart_rate, v.blood_pressure_sys, v.blood_pressure_dia,
       v.temperature_c, v.spo2_pct, v._ingested_at
FROM vitals v
JOIN patients p ON v.patient_id = p.id AND p._is_current = true
WHERE v._is_current = true
ORDER BY v._ingested_at DESC;

-- 3. Patients with critical vitals (current readings outside normal range)
SELECT p.mrn, p.name,
       v.heart_rate, v.blood_pressure_sys, v.temperature_c, v.spo2_pct
FROM vitals v
JOIN patients p ON v.patient_id = p.id AND p._is_current = true
WHERE v._is_current = true
  AND (v.heart_rate > 120 OR v.heart_rate < 50
       OR v.temperature_c > 38.5
       OR v.spo2_pct < 92
       OR v.blood_pressure_sys > 180);

-- 4. Deequ metrics summary — batch quality over time
SELECT table_name, metric_name, column_name,
       AVG(metric_value) as avg_metric,
       MIN(metric_value) as min_metric,
       COUNT(*) as batch_count,
       SUM(CASE WHEN passed = 'false' THEN 1 ELSE 0 END) as failed_batches
FROM dq_metrics
GROUP BY table_name, metric_name, column_name
ORDER BY table_name, metric_name;

-- 5. Patient status change history (SCD2 timeline)
SELECT id, mrn, name, status, _effective_from, _effective_to, _is_current, _operation
FROM patients
WHERE mrn = 'MRN-000001'
ORDER BY _effective_from;

-- 6. Vitals trend for a specific patient
SELECT v.heart_rate, v.blood_pressure_sys, v.blood_pressure_dia,
       v.temperature_c, v.spo2_pct, v._ingested_at
FROM vitals v
JOIN patients p ON v.patient_id = p.id
WHERE p.mrn = 'MRN-000001' AND p._is_current = true
ORDER BY v._ingested_at;

-- 7. Patient completeness check — any missing MRNs?
SELECT COUNT(*) as total_patients,
       SUM(CASE WHEN mrn IS NULL THEN 1 ELSE 0 END) as missing_mrn,
       SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) as missing_name
FROM patients
WHERE _is_current = true;

-- 8. Vitals volume by hour
SELECT date_trunc('hour', _ingested_at) as hour,
       COUNT(*) as vital_readings,
       AVG(heart_rate) as avg_hr,
       AVG(temperature_c) as avg_temp
FROM vitals
WHERE _is_current = true
GROUP BY date_trunc('hour', _ingested_at)
ORDER BY hour DESC
LIMIT 24;
