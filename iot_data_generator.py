"""
IoT Fleet Data Generator Lambda - Robust Version.

Simulates realistic IoT fleet environment:
- 10-20 vehicles with persistent state
- Realistic GPS movement along NYC streets
- Telemetry every 1-2 seconds per vehicle
- Delivery lifecycle with proper status transitions
- ~5% bad sensor data to trigger DQ rules

Usage:
  {"action": "seed", "vehicles": 10, "drivers": 8}
  {"action": "generate", "duration_seconds": 300, "interval_seconds": 1}
  {"action": "burst", "records": 500}
"""

import json
import os
import random
import string
import time
import psycopg2
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone

# Default credentials (fallback for local testing)
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_NAME = "etldb"
DEFAULT_DB_USER = "postgres"
DEFAULT_DB_PASS = "Amazon123"

_cached_credentials = None


def get_db_credentials():
    """Fetch database credentials from AWS Secrets Manager."""
    global _cached_credentials
    
    if _cached_credentials is not None:
        return _cached_credentials
    
    secret_arn = os.environ.get("RDS_SECRET_ARN")
    
    if secret_arn:
        try:
            client = boto3.client("secretsmanager")
            response = client.get_secret_value(SecretId=secret_arn)
            secret_data = json.loads(response.get("SecretString", "{}"))
            _cached_credentials = {
                "host": secret_data.get("host", DEFAULT_DB_HOST),
                "dbname": secret_data.get("dbname", DEFAULT_DB_NAME),
                "username": secret_data.get("username", DEFAULT_DB_USER),
                "password": secret_data.get("password", DEFAULT_DB_PASS),
            }
            return _cached_credentials
        except Exception as e:
            print(f"Warning: Failed to get secret: {e}")
    
    _cached_credentials = {
        "host": os.environ.get("DB_HOST", DEFAULT_DB_HOST),
        "dbname": os.environ.get("DB_NAME", DEFAULT_DB_NAME),
        "username": os.environ.get("DB_USER", DEFAULT_DB_USER),
        "password": os.environ.get("DB_PASS", DEFAULT_DB_PASS),
    }
    return _cached_credentials


# NYC area zones for realistic routes
ZONES = {
    "manhattan": {"lat": (40.70, 40.82), "lon": (-74.02, -73.93)},
    "brooklyn": {"lat": (40.57, 40.70), "lon": (-74.04, -73.85)},
    "queens": {"lat": (40.68, 40.78), "lon": (-73.96, -73.70)},
}

VEHICLE_STATES = {}
ACTIVE_DELIVERIES = {}


def get_connection():
    """Get database connection."""
    creds = get_db_credentials()
    return psycopg2.connect(
        host=creds["host"],
        database=creds["dbname"],
        user=creds["username"],
        password=creds["password"]
    )


def utc_now():
    return datetime.now(timezone.utc)


def generate_vin():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=17))


def generate_plate():
    return f"{''.join(random.choices(string.ascii_uppercase, k=3))}-{random.randint(1000, 9999)}"


def init_vehicle_state(zone="manhattan"):
    z = ZONES.get(zone, ZONES["manhattan"])
    return {
        "lat": random.uniform(z["lat"][0], z["lat"][1]),
        "lon": random.uniform(z["lon"][0], z["lon"][1]),
        "speed": random.uniform(0, 40),
        "fuel": random.uniform(50, 95),
        "odometer": random.uniform(10000, 150000),
        "heading": random.uniform(0, 360),
        "engine_temp": random.uniform(80, 95),
        "zone": zone,
        "is_moving": random.choice([True, True, True, False]),
    }


def seed_vehicles(conn, count=10):
    """Create seed vehicles."""
    cur = conn.cursor()
    makes_models = [
        ("Ford", "Transit"), ("Ford", "E-Transit"), ("Toyota", "Hiace"),
        ("Mercedes", "Sprinter"), ("Chevrolet", "Express"), ("RAM", "ProMaster"),
    ]
    fuel_types = ["diesel", "diesel", "electric", "hybrid", "gasoline"]
    zones = list(ZONES.keys())
    
    created = 0
    for i in range(count):
        make, model = random.choice(makes_models)
        zone = random.choice(zones)
        try:
            cur.execute("""
                INSERT INTO vehicles (vin, license_plate, make, model, year, fuel_type, status)
                VALUES (%s, %s, %s, %s, %s, %s, 'active')
                RETURNING id
            """, (generate_vin(), generate_plate(), make, model,
                  random.randint(2018, 2025), random.choice(fuel_types)))
            vid = cur.fetchone()[0]
            VEHICLE_STATES[vid] = init_vehicle_state(zone)
            created += 1
            conn.commit()
        except psycopg2.IntegrityError:
            conn.rollback()
    cur.close()
    return created


def seed_drivers(conn, count=8):
    """Create seed drivers."""
    cur = conn.cursor()
    first_names = ["John", "Maria", "James", "Sarah", "Michael", "Emily", "Robert", "Jennifer"]
    last_names = ["Smith", "Garcia", "Wilson", "Johnson", "Brown", "Davis", "Miller", "Taylor"]
    
    created = 0
    for i in range(count):
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
        emp_id = f"DRV{random.randint(10000, 99999)}"
        try:
            cur.execute("""
                INSERT INTO drivers (employee_id, name, license_number, phone, status, safety_score)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (emp_id, name, f"DL{random.randint(100000, 999999)}",
                  f"+1{random.randint(2000000000, 9999999999)}",
                  random.choice(["available", "available", "on_duty"]),
                  round(random.uniform(70, 100), 1)))
            created += 1
            conn.commit()
        except psycopg2.IntegrityError:
            conn.rollback()
    cur.close()
    return created


def simulate_movement(state):
    """Simulate realistic vehicle movement."""
    import math
    
    if not state["is_moving"]:
        if random.random() < 0.2:
            state["is_moving"] = True
            state["speed"] = random.uniform(5, 25)
        else:
            state["speed"] = 0
            return state
    
    if random.random() < 0.1:
        state["is_moving"] = False
        state["speed"] = 0
        return state
    
    speed_delta = random.uniform(-8, 12)
    state["speed"] = max(0, min(80, state["speed"] + speed_delta))
    
    if state["speed"] > 0:
        distance_km = state["speed"] / 3600
        
        if random.random() < 0.15:
            state["heading"] = (state["heading"] + random.choice([-90, -45, 45, 90])) % 360
        
        heading_rad = math.radians(state["heading"])
        state["lat"] += distance_km * 0.009 * math.cos(heading_rad)
        state["lon"] += distance_km * 0.012 * math.sin(heading_rad)
        
        z = ZONES.get(state["zone"], ZONES["manhattan"])
        state["lat"] = max(z["lat"][0], min(z["lat"][1], state["lat"]))
        state["lon"] = max(z["lon"][0], min(z["lon"][1], state["lon"]))
        
        state["odometer"] += distance_km
        state["fuel"] = max(5, state["fuel"] - 0.01 - (state["speed"] / 1000))
        state["engine_temp"] += random.uniform(-0.5, 0.8)
        state["engine_temp"] = max(75, min(105, state["engine_temp"]))
    
    return state


def generate_telemetry_batch(conn, vehicle_ids, bad_data_pct=5):
    """Generate telemetry for all vehicles."""
    cur = conn.cursor()
    inserted = 0
    bad_count = 0
    now = utc_now()
    
    values = []
    for vid in vehicle_ids:
        if vid not in VEHICLE_STATES:
            VEHICLE_STATES[vid] = init_vehicle_state()
        
        state = simulate_movement(VEHICLE_STATES[vid])
        is_bad = random.randint(1, 100) <= bad_data_pct
        
        if is_bad:
            bad_type = random.choice(["gps", "speed", "fuel"])
            if bad_type == "gps":
                lat = random.uniform(200, 500)
                lon = random.uniform(-500, -200)
            else:
                lat, lon = state["lat"], state["lon"]
                if bad_type == "speed":
                    state["speed"] = random.uniform(350, 500)
                else:
                    state["fuel"] = random.uniform(110, 150)
            bad_count += 1
        else:
            lat, lon = state["lat"], state["lon"]
        
        harsh_brake = state["speed"] > 40 and random.random() < 0.03
        harsh_accel = state["speed"] < 20 and state["is_moving"] and random.random() < 0.03
        
        values.append((
            vid, now, lat, lon, round(state["speed"], 1), round(state["fuel"], 2),
            round(state["engine_temp"], 1), round(state["odometer"], 1),
            "running" if state["is_moving"] else "idle", harsh_brake, harsh_accel
        ))
        inserted += 1
    
    cur.executemany("""
        INSERT INTO vehicle_telemetry 
        (vehicle_id, recorded_at, latitude, longitude, speed_kmh, fuel_level_pct,
         engine_temp_c, odometer_km, engine_status, harsh_braking, harsh_acceleration)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, values)
    conn.commit()
    cur.close()
    return inserted, bad_count


def create_delivery(conn, vehicle_ids, driver_ids):
    """Create a new delivery."""
    cur = conn.cursor()
    streets = ["Main St", "Oak Ave", "Park Blvd", "Broadway", "5th Ave", "Madison Ave"]
    
    vid = random.choice(vehicle_ids)
    did = random.choice(driver_ids)
    
    cur.execute("""
        INSERT INTO deliveries 
        (vehicle_id, driver_id, status, pickup_address, delivery_address,
         scheduled_time, customer_name, customer_phone)
        VALUES (%s, %s, 'assigned', %s, %s, %s, %s, %s)
        RETURNING id
    """, (vid, did,
          f"{random.randint(1, 999)} {random.choice(streets)}, New York, NY",
          f"{random.randint(1, 999)} {random.choice(streets)}, New York, NY",
          utc_now() + timedelta(hours=random.randint(1, 6)),
          f"Customer {random.randint(1000, 9999)}",
          f"+1{random.randint(2000000000, 9999999999)}"))
    delivery_id = cur.fetchone()[0]
    ACTIVE_DELIVERIES[delivery_id] = {"status": "assigned", "created": utc_now()}
    conn.commit()
    cur.close()
    return 1


def update_deliveries(conn):
    """Progress deliveries through lifecycle."""
    cur = conn.cursor()
    transitions = [
        ("assigned", "picked_up", 0.3),
        ("picked_up", "in_transit", 0.4),
        ("in_transit", "delivered", 0.2),
    ]
    
    updated = 0
    for old_status, new_status, probability in transitions:
        if random.random() < probability:
            cur.execute("""
                UPDATE deliveries SET status = %s, updated_at = NOW()
                WHERE id IN (
                    SELECT id FROM deliveries WHERE status = %s 
                    ORDER BY created_at LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
            """, (new_status, old_status))
            updated += cur.rowcount
    
    conn.commit()
    cur.close()
    return updated


def generate_alerts(conn, vehicle_ids, driver_ids):
    """Generate contextual alerts based on vehicle states."""
    cur = conn.cursor()
    alerts_created = 0
    
    for vid in vehicle_ids:
        state = VEHICLE_STATES.get(vid)
        if not state:
            continue
        
        alert_type = None
        severity = None
        message = None
        
        if state["speed"] > 70 and random.random() < 0.3:
            alert_type = "speeding"
            severity = "warning"
            message = f"Vehicle exceeding speed limit: {state['speed']:.1f} km/h"
        elif state["fuel"] < 20 and random.random() < 0.4:
            alert_type = "low_fuel"
            severity = "critical" if state["fuel"] < 10 else "warning"
            message = f"Low fuel warning: {state['fuel']:.1f}%"
        elif state["engine_temp"] > 100 and random.random() < 0.5:
            alert_type = "engine_overheat"
            severity = "critical"
            message = f"Engine temperature high: {state['engine_temp']:.1f}C"
        elif random.random() < 0.02:
            alert_type = "harsh_braking"
            severity = "info"
            message = "Harsh braking event detected"
        
        if alert_type:
            cur.execute("""
                INSERT INTO alerts 
                (vehicle_id, driver_id, alert_type, severity, message, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (vid, random.choice(driver_ids), alert_type, severity, message,
                  state["lat"], state["lon"]))
            alerts_created += 1
    
    conn.commit()
    cur.close()
    return alerts_created


def lambda_handler(event, context):
    """Lambda entry point."""
    action = event.get("action", "generate")
    
    conn = get_connection()
    results = {"action": action}
    
    try:
        if action == "seed":
            vehicle_count = event.get("vehicles", 10)
            driver_count = event.get("drivers", 8)
            results["vehicles_created"] = seed_vehicles(conn, vehicle_count)
            results["drivers_created"] = seed_drivers(conn, driver_count)
            
        elif action == "burst":
            record_count = event.get("records", 500)
            bad_pct = event.get("bad_data_pct", 5)
            
            cur = conn.cursor()
            cur.execute("SELECT id FROM vehicles WHERE status = 'active'")
            vehicle_ids = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT id FROM drivers")
            driver_ids = [r[0] for r in cur.fetchall()]
            cur.close()
            
            if not vehicle_ids or not driver_ids:
                return {"statusCode": 400, "body": "No vehicles/drivers. Run action=seed first."}
            
            stats = {"telemetry": 0, "bad_data": 0, "deliveries": 0, "alerts": 0}
            iterations = record_count // len(vehicle_ids)
            for _ in range(iterations):
                t, b = generate_telemetry_batch(conn, vehicle_ids, bad_pct)
                stats["telemetry"] += t
                stats["bad_data"] += b
                if random.random() < 0.1:
                    stats["deliveries"] += create_delivery(conn, vehicle_ids, driver_ids)
                if random.random() < 0.05:
                    stats["alerts"] += generate_alerts(conn, vehicle_ids, driver_ids)
            results.update(stats)
            
        elif action == "generate":
            duration = event.get("duration_seconds", 60)
            interval = event.get("interval_seconds", 1)
            bad_pct = event.get("bad_data_pct", 5)
            
            cur = conn.cursor()
            cur.execute("SELECT id FROM vehicles WHERE status = 'active'")
            vehicle_ids = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT id FROM drivers")
            driver_ids = [r[0] for r in cur.fetchall()]
            cur.close()
            
            if not vehicle_ids or not driver_ids:
                return {"statusCode": 400, "body": "No vehicles/drivers. Run action=seed first."}
            
            start = time.time()
            stats = {"telemetry": 0, "bad_data": 0, "deliveries_created": 0,
                     "deliveries_updated": 0, "alerts": 0}
            iteration = 0
            
            while time.time() - start < duration:
                t, b = generate_telemetry_batch(conn, vehicle_ids, bad_pct)
                stats["telemetry"] += t
                stats["bad_data"] += b
                
                if iteration % 5 == 0:
                    stats["deliveries_created"] += create_delivery(conn, vehicle_ids, driver_ids)
                if iteration % 3 == 0:
                    stats["deliveries_updated"] += update_deliveries(conn)
                if iteration % 4 == 0:
                    stats["alerts"] += generate_alerts(conn, vehicle_ids, driver_ids)
                
                iteration += 1
                time.sleep(interval)
            
            results.update(stats)
            results["duration_seconds"] = round(time.time() - start, 1)
            results["iterations"] = iteration
            results["vehicles_active"] = len(vehicle_ids)
            
    except Exception as e:
        return {"statusCode": 500, "body": str(e)}
    finally:
        conn.close()
    
    return {"statusCode": 200, "body": json.dumps(results)}
