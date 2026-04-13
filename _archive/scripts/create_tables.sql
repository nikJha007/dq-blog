-- IoT Fleet Management Tables for Streaming ETL Demo

-- vehicles
CREATE TABLE IF NOT EXISTS vehicles (
    id SERIAL PRIMARY KEY,
    vin VARCHAR(17) NOT NULL UNIQUE,
    license_plate VARCHAR(20),
    make VARCHAR(50),
    model VARCHAR(50),
    year INTEGER,
    fuel_type VARCHAR(20) DEFAULT 'diesel',
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- drivers
CREATE TABLE IF NOT EXISTS drivers (
    id SERIAL PRIMARY KEY,
    employee_id VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    license_number VARCHAR(50),
    phone VARCHAR(20),
    status VARCHAR(20) DEFAULT 'available',
    safety_score DECIMAL(3,1) DEFAULT 100.0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- vehicle_telemetry
CREATE TABLE IF NOT EXISTS vehicle_telemetry (
    id SERIAL PRIMARY KEY,
    vehicle_id INTEGER REFERENCES vehicles(id),
    recorded_at TIMESTAMP NOT NULL,
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),
    speed_kmh DECIMAL(5,1),
    fuel_level_pct DECIMAL(5,2),
    engine_temp_c DECIMAL(5,1),
    odometer_km DECIMAL(10,1),
    engine_status VARCHAR(20),
    harsh_braking BOOLEAN DEFAULT FALSE,
    harsh_acceleration BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);


-- deliveries
CREATE TABLE IF NOT EXISTS deliveries (
    id SERIAL PRIMARY KEY,
    vehicle_id INTEGER REFERENCES vehicles(id),
    driver_id INTEGER REFERENCES drivers(id),
    status VARCHAR(20) DEFAULT 'assigned',
    pickup_address TEXT,
    delivery_address TEXT,
    scheduled_time TIMESTAMP,
    actual_pickup_time TIMESTAMP,
    actual_delivery_time TIMESTAMP,
    customer_name VARCHAR(255),
    customer_phone VARCHAR(20),
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- alerts
CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    vehicle_id INTEGER REFERENCES vehicles(id),
    driver_id INTEGER REFERENCES drivers(id),
    alert_type VARCHAR(50),
    severity VARCHAR(20),
    message TEXT,
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_telemetry_vehicle_id ON vehicle_telemetry(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_recorded_at ON vehicle_telemetry(recorded_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_status ON deliveries(status);
CREATE INDEX IF NOT EXISTS idx_alerts_vehicle_id ON alerts(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts(created_at);
