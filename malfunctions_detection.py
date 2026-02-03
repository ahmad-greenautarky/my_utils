from datetime import datetime, timezone
import pandas as pd
import influxdb
import os
import sys

# Add the parent directory to Python path (C:\)
current_dir = os.path.dirname(os.path.abspath(__file__))
app_dir = os.path.dirname(current_dir)  # Gets the 'app' directory
project_root = os.path.dirname(app_dir)  # Gets the 'C:\' directory
sys.path.append(project_root)

INFLUXDB_HOST = '100.72.59.14'
INFLUXDB_PORT = 8086
INFLUXDB_USERNAME = 'ga_influx_admin'
INFLUXDB_PASSWORD = '3db09545b88ee742e2f4aefd003a691d'
INFLUXDB_DATABASE = 'ga_homeassistant_db'
INFLUXDB_GD_DATABASE = "gd_data"
DURATION = '7d'

# --- Thresholds (radiator/episode) ---
VALVE_OPEN_TH = 80      # % open = heating
VALVE_CLOSED_TH = 10    # % closed
TEMP_RISE_MIN = 0.3     # °C expected increase
DRIFT_TH = 2.0          # °C difference between thermostat & room
OVERHEAT_MARGIN = 0.5   # °C above setpoint considered overheating
VALID_MIN_DURATION = 30

# --- Thresholds (T&H sensor) ---
TEMP_MIN_REALISTIC = 15.0
TEMP_MAX_REALISTIC = 30.0
HUMI_MIN_REALISTIC = 30.0
HUMI_MAX_REALISTIC = 80.0
SENSOR_OFFLINE_HOURS = 6

pd.options.mode.chained_assignment = None  # default='warn'

def sensors_table(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USER, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE):
    # Initialize the InfluxDB client
    client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST,
                                   port=INFLUXDB_PORT,
                                   username=INFLUXDB_USER,
                                   password=INFLUXDB_PASSWORD,
                                   database=INFLUXDB_GD_DATABASE)

    # Query data from measurement1
    query1 = 'SELECT "room_id" , "sensor_id", "floor", "device_uuid", "room_type", "sensor_location" FROM "gd_data"."autogen"."sensor_info"'
    result1 = client_influx.query(query1)

    # Query data from measurement2
    query2 = 'SELECT "sensor_type", "ieee_address", "device_name" FROM "gd_data"."autogen"."sensor_id"'
    result2 = client_influx.query(query2)

    # Convert result to a list of dictionaries
    points1 = list(result1.get_points())
    points2 = list(result2.get_points())

    # Close the client
    client_influx.close()

    # Join the two datasets on 'field_common'
    joined_data = []

    # Create a dictionary for fast lookup of points2 by 'field_common'
    points2_dict = {point['ieee_address']: point for point in points2}

    # Iterate over points1 and join with points2 based on 'field_common'
    for point1 in points1:
        field_common_value = point1['sensor_id'].strip()
        if field_common_value in points2_dict:
            point2 = points2_dict[field_common_value]
            joined_data.append({
                "sensor_id": field_common_value,
                "room_id": str(point1['room_id']),
                "sensor_location": str(point1['sensor_location']),
                "room_type": point1['room_type'],
                "floor": str(point1['floor']),
                "device_uuid": point1['device_uuid'],
                "sensor_type": point2['sensor_type'],
                "device_name": str(point2['device_name'])
            })
    return joined_data


def database_exists(client, dbname):
    existing_databases = client.get_list_database()
    return any(db['name'] == dbname for db in existing_databases)

# Function to connect to the database or create it if it doesn't exist
def connect_or_create_database(host, port, user, password, dbname):
    client = influxdb.InfluxDBClient(host, port, user, password)
    if database_exists(client, dbname):
        print(f"Database '{dbname}' already exists.")
    else:
        client.create_database(dbname)
        print(f"Database '{dbname}' created successfully.")
    return influxdb.InfluxDBClient(host, port, user, password, dbname)

# Helper functions to get current room temperature and humidity
def get_current_room_temperature(room, sensor_table):
    temp_sensor_id = [sensor['sensor_id'] for sensor in sensor_table if sensor['room_id'] == room and (sensor['sensor_type'] == "Smart air house keeper" or sensor['sensor_type'] == "Temperature and humidity sensor with screen" or sensor['sensor_type'] == "Temperature and humidity sensor") and sensor['room_type'] == 'Active']
    
    if temp_sensor_id:
        client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST,
                               port=INFLUXDB_PORT,
                               username=INFLUXDB_USERNAME,
                               password=INFLUXDB_PASSWORD,
                               database=INFLUXDB_DATABASE)
        
        entity_id = temp_sensor_id[0] + '_temperature'
        query_room_temp = f'''SELECT last("value") AS "room_temp" FROM "ga_homeassistant_db"."autogen"."°C" WHERE time > now() - 12h AND "entity_id"=\'{entity_id}\' '''
        room_temp = client_influx.query(query_room_temp)
        room_temp = pd.DataFrame(room_temp.get_points())
        
        client_influx.close()
        return room_temp['room_temp'].values[0] if not room_temp.empty else None
    return None

def get_current_room_humidity(room, sensor_table):
    temp_sensor_id = [sensor['sensor_id'] for sensor in sensor_table if sensor['room_id'] == room and (sensor['sensor_type'] == "Smart air house keeper" or sensor['sensor_type'] == "Temperature and humidity sensor with screen" or sensor['sensor_type'] == "Temperature and humidity sensor") and sensor['room_type'] == 'Active']
    
    if temp_sensor_id:
        client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST,
                               port=INFLUXDB_PORT,
                               username=INFLUXDB_USERNAME,
                               password=INFLUXDB_PASSWORD,
                               database=INFLUXDB_DATABASE)
        
        entity_id = temp_sensor_id[0] + '_humidity'
        query_room_humi = f'''SELECT last("value") AS "room_humi" FROM "ga_homeassistant_db"."autogen"."%" WHERE time > now() - 12h AND "entity_id"=\'{entity_id}\' '''
        room_humi = client_influx.query(query_room_humi)
        room_humi = pd.DataFrame(room_humi.get_points())
        
        client_influx.close()
        return room_humi['room_humi'].values[0] if not room_humi.empty else None
    return None

def get_sensor_last_seen(room, sensor_table):
    temp_sensor_id = [sensor['sensor_id'] for sensor in sensor_table if sensor['room_id'] == room and (sensor['sensor_type'] == "Smart air house keeper" or sensor['sensor_type'] == "Temperature and humidity sensor with screen" or sensor['sensor_type'] == "Temperature and humidity sensor") and sensor['room_type'] == 'Active']
    
    if temp_sensor_id:
        client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST,
                               port=INFLUXDB_PORT,
                               username=INFLUXDB_USERNAME,
                               password=INFLUXDB_PASSWORD,
                               database=INFLUXDB_DATABASE)
        
        # Check temperature sensor last seen
        entity_id = temp_sensor_id[0] + '_temperature'
        query_last_seen = f'''SELECT last("value") AS "last_value", ("time") AS "last_time" FROM "ga_homeassistant_db"."autogen"."°C" WHERE "entity_id"=\'{entity_id}\' '''
        result = client_influx.query(query_last_seen)
        result_df = pd.DataFrame(result.get_points())
        
        client_influx.close()
        
        if not result_df.empty and 'last_time' in result_df.columns:
            return pd.to_datetime(result_df['last_time'].values[0])
    
    return None

def get_last_alert_timestamp(room, flag):
    """
    Check when was the last time this flag was triggered and an email was sent.
    
    Args:
        room: Room identifier
        flag: Flag name
    
    Returns:
        dict with last alert info or None if no previous alert
    """
    try:
        client_influx = influxdb.InfluxDBClient(
            host=INFLUXDB_HOST,
            port=INFLUXDB_PORT,
            username=INFLUXDB_USERNAME,
            password=INFLUXDB_PASSWORD,
            database=INFLUXDB_GD_DATABASE
        )
        
        query = f'''
            SELECT triggered, malfunction_status, email_sent
            FROM "gd_data"."autogen"."malfunction"
            WHERE room = '{room}' 
            AND flag = '{flag}'
            AND triggered = 1
            AND email_sent = 1
            ORDER BY time DESC
            LIMIT 1
        '''
        
        result = client_influx.query(query)
        client_influx.close()
        
        if result and len(result) > 0:
            points = list(result.get_points())
            if len(points) > 0:
                last_alert = points[0]
                timestamp = last_alert.get('time')
                if timestamp:
                    last_alert_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    
                    return {
                        'found': True,
                        'timestamp': timestamp,
                        'timestamp_parsed': last_alert_time,
                        'malfunction_status': last_alert.get('malfunction_status')
                    }
        
        return {'found': False}
        
    except Exception as e:
        print(f"✗ Error querying last alert: {e}")
        return {'found': False}

def send_alert_email(room, radiator, flag, malfunction_status, row=None):
    """
    Send alert email for triggered flag.
    """
    try:
        subject = f"🚨 Malfunction Alert: {flag.replace('_', ' ').title()} in Room {room}"
        
        # Create detailed body with available data
        body = f"""
HEATING MALFUNCTION ALERT

Detected: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC

LOCATION
  Room:      {room}
  Radiator:  {radiator}

PROBLEM
  {flag.replace('_', ' ').title()}

STATUS
  {malfunction_status}
"""
        
        # Add additional data if row is provided
        if row is not None:
            if 'start_room_temperature' in row and 'end_room_temperature' in row:
                body += f"""
TEMPERATURE DATA
  Room Temp Start : {row.get('start_room_temperature')}
  Room Temp End   : {row.get('end_room_temperature')}
  Temp Difference : {row.get('room_temp_delta', 'N/A')}
"""
            
            if 'setpoint_last' in row:
                body += f"  Target Temp     : {row.get('setpoint_last')}\n"
            
            if 'valve_open_median' in row:
                body += f"  Valve Open (%)  : {row.get('valve_open_median')}\n"
            
            if 'duration_min' in row:
                body += f"  Duration (min)  : {row.get('duration_min')}\n"
        
        # Send email using your existing email function
        #send_email_alert(subject, body)
        
        print(f"✓ Alert email sent for room={room}, radiator={radiator}, flag={flag}")
        return True
        
    except Exception as e:
        print(f"✗ Error sending email: {e}")
        return False

def save_malfunctions_to_influxdb(df, room, radiator):
    """
    Save malfunction flags to InfluxDB v1 measurement 'malfunction'.
    Checks last alert timestamp and sends email only if it's a new alert.
    
    Args:
        df: DataFrame with malfunction data
        room: Room identifier
        radiator: Radiator identifier
    """
    try:
        client_influx = influxdb.InfluxDBClient(
            host=INFLUXDB_HOST,
            port=INFLUXDB_PORT,
            username=INFLUXDB_USERNAME,
            password=INFLUXDB_PASSWORD,
            database=INFLUXDB_GD_DATABASE
        )
        
        points = []
        
        # Define all flag columns (existing + new)
        flag_columns = [
            'flag_no_heat_increase',
            'flag_overheating', 
            'flag_sensor_drift',
            'flag_temp_rise_while_off',
            'flag_th_temp_out_of_range',
            'flag_th_humi_out_of_range',
            'flag_th_sensor_offline',
            'flag_heating_but_no_temp_increase_sensor',
            'flag_idle_but_radiator_temp_increase'
        ]
        
        for idx, row in df.iterrows():
            # Use timestamp from your data if available
            timestamp = row.get('start_time')  # Use start_time as timestamp
            if timestamp is None or pd.isna(timestamp):
                timestamp = datetime.utcnow().isoformat() + 'Z'
            elif not isinstance(timestamp, str):
                timestamp = pd.to_datetime(timestamp).isoformat() + 'Z'
            
            # Create a point for each flag
            for flag_col in flag_columns:
                if flag_col in row.index:
                    flag_name = flag_col.replace('flag_', '')
                    flag_triggered = int(bool(row[flag_col]))  # Convert to 0/1
                    
                    email_sent = 0
                    
                    # Check if flag is triggered THIS TIME
                    if flag_triggered == 1:
                        # Get last alert with email sent
                        last_alert = get_last_alert_timestamp(room, flag_name)
                        
                        # Send email only if this is a new alert (no previous one or previous one was different)
                        if not last_alert['found']:
                            # First time this flag triggered - send email
                            if send_alert_email(room, radiator, flag_name, row.get("malfunction_status", "Unknown"), row):
                                email_sent = 1
                        else:
                            # Check if enough time has passed since last alert (e.g., 24 hours)
                            last_alert_time = last_alert.get('timestamp_parsed')
                            if last_alert_time:
                                time_since_last_alert = datetime.utcnow() - last_alert_time
                                if time_since_last_alert.total_seconds() > 24 * 3600:  # 24 hours
                                    if send_alert_email(room, radiator, flag_name, row.get("malfunction_status", "Unknown"), row):
                                        email_sent = 1
                    
                    point = {
                        "measurement": "malfunction",
                        "tags": {
                            "room": str(room),
                            "radiator": str(radiator),
                            "flag": flag_name
                        },
                        "time": timestamp,
                        "fields": {
                            "triggered": flag_triggered,
                            "email_sent": email_sent,
                            "malfunction_status": row.get("malfunction_status", "Unknown"),
                            "duration_min": float(row.get("duration_min", 0)),
                            "valve_open_median": float(row.get("valve_open_median", 0)),
                            "start_room_temp": float(row.get("start_room_temperature", 0)),
                            "end_room_temp": float(row.get("end_room_temperature", 0))
                        }
                    }
                    points.append(point)
        
        # Write points to InfluxDB
        if points:
            client_influx.write_points(points, batch_size=1000)
            print(f"✓ Saved {len(points)} flag records to InfluxDB for room={room}, radiator={radiator}")
        else:
            print(f"No points to save for room={room}, radiator={radiator}")
        
        client_influx.close()
        return True
        
    except Exception as e:
        print(f"✗ Error writing to InfluxDB: {e}")
        return False

# RADIATOR STATUS
def heating_sessions(client_influx, room, radiator, temp_sensor_id):
                    
    query_radiator = f'''SELECT ("start_time") AS "start_time", ("end_time") AS "end_time",
                                ("duration") AS "duration_hr", ("average_valve_opening") AS "valve_open_median", 
                                ("start_setpoint") AS "start_setpoint", ("end_setpoint") AS "end_setpoint", 
                                ("start_temperature") AS "start_temperature", ("end_temperature") AS "end_temperature" 
                                FROM "gd_data"."autogen"."heating_sessions" 
                                WHERE time > now() -{DURATION} AND "{room}"=\'{radiator}\' FILL(previous)'''

    radiator_data = client_influx.query(query_radiator)
    heating_session = pd.DataFrame(radiator_data.get_points())

    if not heating_session.empty:

        heating_session['start_time'] = pd.to_datetime(heating_session['start_time'], utc=True)
        heating_session['end_time'] = pd.to_datetime(heating_session['end_time'], utc=True)

        data_list = pd.DataFrame()
                            
        if temp_sensor_id:

            entity_id = temp_sensor_id[0] + '_temperature'
                
            query_room_temp = f'''SELECT mean("value") AS "room_temp" FROM "ga_homeassistant_db"."autogen"."°C" WHERE time > now() -{DURATION}  AND "entity_id"=\'{entity_id}\' GROUP BY time(10s) FILL(previous)'''
            
            room_temp = client_influx.query(query_room_temp)
            room_temp = pd.DataFrame(room_temp.get_points())
            room_temp['time'] = pd.to_datetime(room_temp['time'])

            if data_list.empty:
                data_list = room_temp
            else:
                data_list = pd.merge(data_list, room_temp, on="time", how="outer")

            entity_id = temp_sensor_id[0] + '_humidity'

            query_room_humi = f'''SELECT mean("value") AS "room_humi" FROM "ga_homeassistant_db"."autogen"."%" WHERE time > now() -{DURATION}  AND "entity_id"=\'{entity_id}\' GROUP BY time(10s) FILL(previous)'''
            
            room_humi = client_influx.query(query_room_humi)
            room_humi = pd.DataFrame(room_humi.get_points())
            room_humi['time'] = pd.to_datetime(room_humi['time'])
            if data_list.empty:
                data_list = room_humi
            else:
                data_list = pd.merge(data_list, room_humi, on="time", how="outer")

        
        if not data_list.empty:
            
            data_list = data_list.round(2)
            data_list.ffill(inplace=True)
            data_list.bfill(inplace=True)
            data_list['time'] = pd.to_datetime(data_list['time'], utc=True, format='mixed')

            # Find the closest room temperature for start_time
            heating_session = heating_session.merge(
                data_list.rename(columns={'time': 'start_time'}),
                on='start_time',
                how='left'
            ).rename(columns={ 
                'room_temp': 'start_room_temperature',
                'room_humi': 'start_humidity'})


            heating_session = heating_session.merge(
                data_list.rename(columns={'time': 'end_time'}),
                on='end_time',
                how='left'
            ).rename(columns={ 
                'room_temp': 'end_room_temperature',
                'room_humi': 'end_humidity'})

    return heating_session

# normalize running_state text
def norm_state(x):
    if pd.isna(x):
        return "unknown"
    s = str(x).strip().lower()
    if s in {"heating", "heat", "on", "run", "active", "heating_on"}:
        return "heating"
    if s in {"idle", "off", "standby", "inactive", "heating_off"}:
        return "idle"
    return s

def check_temperature_humidity_flags(df, room, sensor_table):
    """
    Check T&H sensor flags for each row in the dataframe
    """

    client_influx = connect_or_create_database(
        INFLUXDB_HOST,
        INFLUXDB_PORT,
        INFLUXDB_USERNAME,
        INFLUXDB_PASSWORD,
        INFLUXDB_GD_DATABASE
    )

    # Initialize new flag columns
    df["flag_th_temp_out_of_range"] = False
    df["flag_th_humi_out_of_range"] = False
    df["flag_th_sensor_offline"] = False
    df["flag_heating_but_no_temp_increase_sensor"] = False
    df["flag_idle_but_radiator_temp_increase"] = False
    
    # Get current sensor readings
    current_temp = get_current_room_temperature(room, sensor_table)
    current_humi = get_current_room_humidity(room, sensor_table)
    last_seen = get_sensor_last_seen(room, sensor_table)
    
    # Check if room is a bathroom (skip humidity checks)
    is_bathroom = False
    room_sensors = [sensor for sensor in sensor_table if sensor['room_id'] == room]
    print(room_sensors)
    if room_sensors and 'room_id' in room_sensors[0]:
        room_id = room_sensors[0].get('room_id', '').lower()
        is_bathroom = room_id in ['bathroom', 'bad', 'wc', 'toilet', 'badezimmer']

    radiator_id_list = [sensor['sensor_id'] for sensor in sensor_table if sensor['room_id'] == room and sensor['sensor_type'] == "Zigbee thermostatic radiator valve" and sensor['room_type'] == 'Active']
    
    
    # Check sensor offline
    if last_seen:
        offline_cutoff = datetime.now(timezone.utc) - pd.Timedelta(hours=SENSOR_OFFLINE_HOURS)
        print(f"Sensor last seen: {last_seen}, Offline cutoff: {offline_cutoff}")
        df["flag_th_sensor_offline"] = (last_seen < offline_cutoff)
    else:
        # If we can't get last_seen, mark as offline
        df["flag_th_sensor_offline"] = True
    
    # Check temperature out of range
    if current_temp is not None:
        df["flag_th_temp_out_of_range"] = (current_temp < TEMP_MIN_REALISTIC) | (current_temp > TEMP_MAX_REALISTIC)
    
    # Check humidity out of range (skip for bathrooms)
    if current_humi is not None and not is_bathroom:
        df["flag_th_humi_out_of_range"] = (current_humi < HUMI_MIN_REALISTIC) | (current_humi > HUMI_MAX_REALISTIC)
    
    # Real-time check: heating but no temperature increase from sensor
    for radiator_id in radiator_id_list:

        query_radiator_data = f'SELECT last("local_temperature_calibration") AS "local_temperature_calibration",  ("local_temperature") AS "local_temperature", ("running_state") AS "running_state" FROM "gd_data"."autogen"."radiator_data" WHERE time < now() - 1d AND "{room}"=\'{radiator_id}\''
        radiator_data = client_influx.query(query_radiator_data)
        radiator_data = pd.DataFrame(list(radiator_data.get_points()))

        print(f"Radiator data for room {room}, radiator {radiator_id}:")
        print(radiator_data)
        print(radiator_data.iloc[0])

        if not radiator_data.empty: 

            if radiator_data.iloc[0]['running_state'] == 'heating':

                query_last_idle = f'SELECT ("local_temperature_calibration") AS "mean_local_temperature_calibration",  ("local_temperature") AS "mean_local_temperature", ("running_state") AS "running_state" FROM "gd_data"."autogen"."radiator_data" WHERE time < now() - 1d AND "{room}"=\'{radiator_id}\' AND "running_state" != \'heating\''
                radiator_data_idle = client_influx.query(query_last_idle)
                radiator_data_idle = pd.DataFrame(list(radiator_data_idle.get_points()))

                if not radiator_data_idle.empty:
                    last_idle_time = pd.to_datetime(radiator_data_idle['time']).max()
                    durattion_since_idle = (pd.to_datetime(radiator_data['time']).max() - last_idle_time).total_seconds() / 3600.0

                    if durattion_since_idle >= 0.5:  # at least 30 minutes of heating

                        if radiator_data.iloc[0]['local_temperature_calibration'] > -2.5:

                            df["flag_heating_but_no_temp_increase_sensor"] = True
                            print(f"Flag set: Heating but no temp increase from sensor in room {room}, radiator {radiator_id}")
            
            elif radiator_data.iloc[0]['running_state'] == 'idle':
                
                query_last_heating = f'SELECT ("local_temperature_calibration") AS "mean_local_temperature_calibration",  ("local_temperature") AS "mean_local_temperature", ("running_state") AS "running_state" FROM "gd_data"."autogen"."radiator_data" WHERE time < now() - 1d AND "{room}"=\'{radiator_id}\' AND "running_state" != \'idle\''
                radiator_data_heating = client_influx.query(query_last_heating)
                radiator_data_heating = pd.DataFrame(list(radiator_data_heating.get_points()))

                if not radiator_data_heating.empty:
                    last_heating_time = pd.to_datetime(radiator_data_heating['time']).max()
                    duration_since_heating = (pd.to_datetime(radiator_data['time']).max() - last_heating_time).total_seconds() / 3600.0

                    if duration_since_heating >= 0.5:  # at least 30 minutes of idle

                        if radiator_data.iloc[0]['local_temperature_calibration'] < -2.5:

                            df["flag_idle_but_radiator_temp_increase"] = True
                            print(f"Flag set: Heating but no temp increase from sensor in room {room}, radiator {radiator_id}")
            
    
    # # From heating session data
    # # Check heating but no temperature increase from sensor
    # if 'start_room_temperature' in df.columns and 'end_room_temperature' in df.columns:
    #     sensor_temp_delta = df['end_room_temperature'] - df['start_room_temperature']
    #     df["flag_heating_but_no_temp_increase_sensor"] = (
    #         (df.get("valve_open_median", 0) >= VALVE_OPEN_TH) &
    #         (sensor_temp_delta < TEMP_RISE_MIN) &
    #         (df.get("state", "") != "idle")
    #     )
    
    return df

def classify(row):
    issues = []
    if row.get("flag_no_heat_increase", False):
        issues.append("NoHeatIncrease")
    if row.get("flag_overheating", False):
        issues.append("Overheating")
    if row.get("flag_sensor_drift", False):
        issues.append("SensorDrift")
    if row.get("flag_temp_rise_while_off", False):
        issues.append("TempRiseWhileOff")
    if row.get("flag_th_temp_out_of_range", False):
        issues.append("TempOutOfRange")
    if row.get("flag_th_humi_out_of_range", False):
        issues.append("HumidityOutOfRange")
    if row.get("flag_th_sensor_offline", False):
        issues.append("SensorOffline")
    if row.get("flag_heating_but_no_temp_increase_sensor", False):
        issues.append("NoTempIncreaseSensor")
    if row.get("flag_idle_but_radiator_temp_increase", False):
        issues.append("IdleButRadiatorTempIncrease")
    return ",".join(issues) if issues else "Normal"

def build_email_from_row(row, room, radiator):
    """
    Builds a human-readable email body for a malfunction event.
    """
    email_body = f"""
HEATING MALFUNCTION ALERT

Detected: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC

LOCATION
  Room:      {room}
  Radiator:  {radiator}

PROBLEM
  {row.get("malfunction_status", "Unknown")}

CURRENT STATUS
  Room Temp Start : {row.get("start_room_temperature")}
  Room Temp End   : {row.get("end_room_temperature")}
  Target Temp     : {row.get("setpoint_last")}
  Temp Difference : {row.get("room_temp_delta")}

  Valve Open (%)  : {row.get("valve_open_median")}
  Duration (min)  : {row.get("duration_min")}
"""
    
    # Add T&H sensor specific information if available
    if row.get("flag_th_temp_out_of_range", False):
        email_body += "\n  ⚠ Temperature sensor reading outside realistic range (15-30°C)"
    
    if row.get("flag_th_humi_out_of_range", False):
        email_body += "\n  ⚠ Humidity sensor reading outside realistic range (30-80%)"
    
    if row.get("flag_th_sensor_offline", False):
        email_body += "\n  ⚠ Temperature/Humidity sensor offline for more than 6 hours"
    
    if row.get("flag_heating_but_no_temp_increase_sensor", False):
        email_body += "\n  ⚠ Heating active but no temperature increase detected by room sensor"

    if row.get("flag_idle_but_radiator_temp_increase", False):
        email_body += "\n  ⚠ Radiator temperature increasing while in idle state"
    
    return email_body

def main():
    client_influx = connect_or_create_database(
        INFLUXDB_HOST,
        INFLUXDB_PORT,
        INFLUXDB_USERNAME,
        INFLUXDB_PASSWORD,
        INFLUXDB_GD_DATABASE
    )

    sensor_table = sensors_table(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)

    rooms_id =  set(sensor['room_id'] for sensor in sensor_table if sensor['room_type'] == 'Active')
        
    for room in rooms_id:

        radiator_id_list = [sensor['sensor_id'] for sensor in sensor_table if sensor['room_id'] == room and sensor['sensor_type'] == "Zigbee thermostatic radiator valve"]
    
        temp_sensor_id = [sensor['sensor_id'] for sensor in sensor_table if sensor['room_id'] == room and (sensor['sensor_type'] == "Smart air house keeper" or sensor['sensor_type'] == "Temperature and humidity sensor with screen" or sensor['sensor_type'] == "Temperature and humidity sensor") and sensor['room_type'] == 'Active']
        
        if radiator_id_list:

            client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD, database=INFLUXDB_GD_DATABASE)

            for radiator in radiator_id_list:

                df = heating_sessions(client_influx, room, radiator, temp_sensor_id)

                if not df.empty:
                    
                    df['room_temp_delta'] = df['end_temperature'] - df['start_temperature']
                    df["duration_min"] = df["duration_hr"] * 60.0
                    df['setpoint_last'] = df['end_setpoint']
                    df['t_room_mean'] = (df['start_room_temperature'] + df['end_room_temperature']) / 2.0
                    df['t_local_mean'] = (df['start_temperature'] + df['end_temperature']) / 2.0
                    df = df[df["duration_min"] >= VALID_MIN_DURATION]
                    
                    # Existing flags
                    df["flag_no_heat_increase"] = (
                        (df["duration_min"] >= VALID_MIN_DURATION) &              
                        (df.get("valve_open_median", 0) >= VALVE_OPEN_TH) &
                        (df.get("room_temp_delta", 0) < TEMP_RISE_MIN) &
                        (df.get("state", "") != "idle")
                    )

                    df["flag_overheating"] = (
                        (df.get("end_room_temperature", 0) > df.get("setpoint_last", 0) + OVERHEAT_MARGIN)
                    )

                    df["flag_sensor_drift"] = (
                        (df.get("t_local_mean", 0) - df.get("t_room_mean", 0)).abs() > DRIFT_TH
                    )

                    df["flag_temp_rise_while_off"] = (
                        (df.get("valve_open_median", 100) <= VALVE_CLOSED_TH) &
                        (df.get("room_temp_delta", 0) > TEMP_RISE_MIN)
                    )
                    
                else: 
                    print(f"No heating session data available for room {room}, radiator {radiator}.")
                    df = pd.DataFrame()

                # New T&H sensor flags
                df = check_temperature_humidity_flags(df, room, sensor_table)

                df["malfunction_status"] = df.apply(classify, axis=1)

                malfunctions_only = df[
                    (df["malfunction_status"] != "Normal") &
                    (df.get("state", "") != "idle")
                ].copy()

                if not malfunctions_only.empty:
                    print(f"⚠ {len(malfunctions_only)} malfunction(s) detected for room {room}, radiator {radiator}")
                    
                    # Print malfunction status counts
                    print(malfunctions_only["malfunction_status"].value_counts())
                    print(f" Filtered malfunctions saved: {len(malfunctions_only)} rows")
                    
                    # Save to InfluxDB with alert logic
                    save_malfunctions_to_influxdb(malfunctions_only, room, radiator)
                    
                    # Also save to CSV for backup
                    malfunctions_only.to_csv(
                        f'detected_malfunction_{room}_{radiator}.csv',
                        index=False
                    )
                    
                    print(f"✓ Malfunction data saved to InfluxDB and CSV for room {room}, radiator {radiator}")
                else:
                    print(f"✅ No malfunctions detected for room {room}, radiator {radiator}.")

        else:
            print(f"No data available for room {room}.")
            continue     

if __name__ == "__main__":
    print("🚀 Starting malfunction alert service...")
    main()