from datetime import datetime, timedelta, timezone
import influxdb
import pandas as pd
import paho.mqtt.client as mqtt
import time
import json
import pytz
import os
from dotenv import load_dotenv
import asyncio
import sys
#from utils import env_loader

# Add the parent directory to Python path (C:\)
current_dir = os.path.dirname(os.path.abspath(__file__))
app_dir = os.path.dirname(current_dir)  # Gets the 'app' directory
project_root = os.path.dirname(app_dir)  # Gets the 'C:\' directory
sys.path.append(project_root)

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
# try:
#     from app.utils.sensors_table import sensors_table
#     print("✅ Import successful!")
# except ImportError as e:
#     print("❌ Import failed:", e)
#     sys.exit(1)

# InfluxDB configurations
INFLUXDB_HOST = '100.92.224.85'
MQTT_HOST = '100.92.224.85'
MQTT_PORT = 1883  # Change if your MQTT broker uses a different port
MQTT_USERNAME = 'ga_zigbee2mqtt'  # Add your MQTT username if needed
MQTT_PASSWORD = 'ga_zigbee2mqtt'
INFLUXDB_PORT = 8086
INFLUXDB_USERNAME = 'ga_influx_admin'
INFLUXDB_PASSWORD = 'e9a5502f66834c314222b64dd8b98918'
INFLUXDB_DATABASE = 'ga_homeassistant_db'
INFLUXDB_GD_DATABASE = "gd_data"
TIMEZONE = 'Europe/Berlin'
TIMESTAMP_COL = 'time'
threshold = 100
LOCAL_TZ = pytz.timezone('Europe/Berlin')
extra_coef_factor = 1.5
extra_preheating_factor = 0.1
extra_factor = 0.2
DEFAULT_HEATING_RATE = 1.0
DEFAULT_COOLING_RATE = -1.0
early_stop = 0.4
early_stop_ai = 0.3

def on_connect(mqtt_client, userdata, flags, rc, state_topic, request_topic,
               request_payload):
    print(f"Connected to MQTT broker with result code {rc}")

    # Subscribe to the state topic to get the response
    mqtt_client.subscribe(state_topic)
    print(f"Subscribed to {state_topic}")

    # Publish the request to get the current state
    mqtt_client.publish(request_topic, json.dumps(request_payload))
    print(f"Published state request to {request_topic}")


# Callback when a message is received from the broker
def on_message(client, userdata, msg, shared_state):
    print(f"Received message from {msg.topic}: {msg.payload.decode()}")

    try:
        # Assuming the payload is a JSON object with multiple attributes
        data = json.loads(msg.payload.decode())

        # Store the local_temperature and local_temperature_calibration value if present
        if "system_mode" in data and "occupied_heating_setpoint" in data:
            shared_state['system_mode'] = data["system_mode"]
            shared_state['occupied_heating_setpoint'] = data["occupied_heating_setpoint"]
            shared_state['valve_opening_degree'] = data["valve_opening_degree"]
            shared_state['running_state'] = data["running_state"]
            shared_state['open_window'] = data["open_window"]
            shared_state['local_temperature'] = data["local_temperature"]
            shared_state['local_temperature_calibration'] = data["local_temperature_calibration"]
            shared_state['weekly_schedule'] = data["weekly_schedule"],
            shared_state['data_received'] = True  # Set the flag to indicate the temperature was received
            print(f"Current system mode: {shared_state['system_mode']}")
        else:
            print("local_temperature attribute not found in the message.")

    except json.JSONDecodeError:
        print("Received message is not in JSON format")

    # Stop the loop since we have the temperature value
    client.loop_stop()
    
def room_temperature(room, sensor_table):

    temp_sensor_id = [sensor['sensor_id'] for sensor in sensor_table if sensor['room_id'] == room and (sensor['sensor_type'] == "Smart air house keeper" or sensor['sensor_type'] == "Temperature and humidity sensor with screen" or sensor['sensor_type'] == "Temperature and humidity sensor") ]
    print(temp_sensor_id)
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
        print('room_temp', room_temp)

        return room_temp['room_temp'].values[0] if not room_temp.empty else None

def room_temperature_last_ai(room, time):

    timestamp = datetime.fromisoformat(time.replace("Z", "+00:00"))

    # Remove microseconds and format back with Z
    formatted_time = timestamp.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    print(f"Room: {room}, Time: {formatted_time}")

    sensor_table = sensors_table(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)

    temp_sensor_id = [sensor['sensor_id'] for sensor in sensor_table if sensor['room_id'] == room and (sensor['sensor_type'] == "Smart air house keeper" or sensor['sensor_type'] == "Temperature and humidity sensor with screen" or sensor['sensor_type'] == "Temperature and humidity sensor") ]
    print(temp_sensor_id)
    if temp_sensor_id:

        client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST,
                               port=INFLUXDB_PORT,
                               username=INFLUXDB_USERNAME,
                               password=INFLUXDB_PASSWORD,
                               database=INFLUXDB_DATABASE)
        
        entity_id = temp_sensor_id[0] + '_temperature'
        query_room_temp = f'''SELECT last("value") AS "room_temp" FROM "ga_homeassistant_db"."autogen"."°C" WHERE time < '{formatted_time}' AND "entity_id"=\'{entity_id}\' '''
        room_temp = client_influx.query(query_room_temp)
        room_temp = pd.DataFrame(room_temp.get_points())
        print('room_temp', room_temp)

        return room_temp['room_temp'].values[0] if not room_temp.empty else None
    
def occupancy_data(occupancy_id_list, occupancy_type, duration, duration_influx):
    
    if occupancy_id_list:
        
        occupancy_data = None

        for occupancy_id in occupancy_id_list:
            
            occupancy_data = None

            if occupancy_type[0] in ['Zigbee occupancy sensor', 'Motion sensor']:
                entity_id_presence = occupancy_id + '_occupancy'
            else:
                entity_id_presence = occupancy_id + '_presence'

            client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD, database=INFLUXDB_DATABASE)

            query_occupancy = f'''SELECT "value" AS "occupancy" FROM {INFLUXDB_DATABASE}."autogen"."state" WHERE  time > now() - {duration_influx} AND "entity_id"=\'{entity_id_presence}\' FILL(previous)'''
            occupancy_data = client_influx.query(query_occupancy)
            occupancy_data = pd.DataFrame(occupancy_data.get_points())

            if not occupancy_data.empty:
                
                if len(occupancy_data) == 1:
                    
                    query_occupancy = f'''SELECT "value" AS "occupancy" FROM {INFLUXDB_DATABASE}."autogen"."state" WHERE  "entity_id"=\'{entity_id_presence}\' LIMIT 2'''
                    occupancy_data_last_two = client_influx.query(query_occupancy)
                    occupancy_data_last_two = pd.DataFrame(occupancy_data_last_two.get_points())
                    
                    if occupancy_data_last_two.iloc[0]['occupancy'] != occupancy_data_last_two.iloc[1]['occupancy']:
                        
                        occupancy_data['time'] = pd.to_datetime(occupancy_data['time']).round('s')
                        occupancy_data['time'] = occupancy_data['time'].dt.tz_convert('Europe/Berlin')
                        occupancy_data = occupancy_data.set_index('time')

                        end_time = pd.Timestamp.now(tz='UTC')
                        end_time = end_time.tz_convert('Europe/Berlin').round('s')
                        start_time = end_time - pd.Timedelta(minutes=duration).round('s')

                        occupancy_data_resampled = occupancy_data.resample('10s').ffill()
                        full_range = pd.date_range(start=start_time, end=end_time, freq='10s', tz='Europe/Berlin')
                        merged_df = occupancy_data_resampled.reindex(full_range, method='ffill')

                        non_nan_values = merged_df['occupancy'].dropna()
                        if not non_nan_values.empty:
                            first_non_nan_value = non_nan_values.iloc[0]
                            opposite_value = 1.0 if first_non_nan_value == 0.0 else 0.0
                        else:
                            first_non_nan_value = 0.0
                            opposite_value = 0.0
                        
                        merged_df['occupancy'] = merged_df['occupancy'].fillna(opposite_value)
                        
                        occupancy_mean = merged_df.occupancy.mean()
                        occupancy_data = occupancy_mean
                        return occupancy_data
    
                    else:
                        return occupancy_data.iloc[0]['occupancy']

                elif len(occupancy_data) > 1:

                    occupancy_data['time'] = pd.to_datetime(occupancy_data['time']).round('s')
                    occupancy_data['time'] = occupancy_data['time'].dt.tz_convert('Europe/Berlin')
                    occupancy_data = occupancy_data.set_index('time')

                    end_time = pd.Timestamp.now(tz='UTC')
                    end_time = end_time.tz_convert('Europe/Berlin').round('s')
                    start_time = end_time - pd.Timedelta(minutes=duration).round('s')

                    occupancy_data_resampled = occupancy_data.resample('10s').ffill()
                    full_range = pd.date_range(start=start_time, end=end_time, freq='10s', tz='Europe/Berlin')
                    merged_df = occupancy_data_resampled.reindex(full_range, method='ffill')

                    non_nan_values = merged_df['occupancy'].dropna()
                    if not non_nan_values.empty:
                        first_non_nan_value = non_nan_values.iloc[0]
                        opposite_value = 1.0 if first_non_nan_value == 0.0 else 0.0
                    else:
                        first_non_nan_value = 0.0
                        opposite_value = 0.0

                    merged_df['occupancy'] = merged_df['occupancy'].fillna(opposite_value)
                    
                    occupancy_mean = merged_df.occupancy.mean()
                    occupancy_data = occupancy_mean
                    return occupancy_data

                else:
                    return occupancy_data.iloc[0]['occupancy']
        
            else:
                try:               
                    query_occupancy = f'''SELECT LAST("value") FROM {INFLUXDB_DATABASE}."autogen"."state" WHERE "entity_id"=\'{entity_id_presence}\''''
                    print(query_occupancy)
                    occupancy_data = client_influx.query(query_occupancy)
                    occupancy_data = list(occupancy_data.get_points())
                    
                    # as the dataframe for the last duration period is empty than the last value is prior to that duration
                    occupancy_data = occupancy_data[0]['last']
                    
                    print(occupancy_data)


                except: print('NO OCCUPANCY DATA AVAILABLE!!')
                    
                if occupancy_data is not None:
                
                    return occupancy_data
    
    # FIXED: Return a default value if no occupancy data is found
    print("Warning: No occupancy data available, returning default value 0.0")
    return 0.0

def occupancy_data_quality(occupancy_id_list, occupancy_type):

    mean_occupancy_data_last_day = occupancy_data(occupancy_id_list, occupancy_type, 540, '9h')

    if mean_occupancy_data_last_day == 1.0:

        return False
    
    else: return True

# def get_radiator_schedule_from_status(radiator_status_data, room):
#     """
#     Get radiator schedule directly from radiator status data
    
#     Parameters:
#     - radiator_status_data (dict): Data from radiator_status() function
#     - room (str): Room identifier
    
#     Returns:
#     - list: Raw schedule data in the same format as radiator_schedule()
#     """
#     if not radiator_status_data or 'weekly_schedule' not in radiator_status_data:
#         return []
    
#     try:
#         # Get day of week
#         day_of_week = datetime.now().strftime("%A").lower()
#         next_day_of_week = (datetime.now() + timedelta(days=1)).strftime("%A").lower()
        
#         # Extract weekly_schedule from the data structure
#         weekly_schedule = radiator_status_data['weekly_schedule']
        
#         # Handle the structure - it appears to be a tuple containing a dict
#         if isinstance(weekly_schedule, tuple) and len(weekly_schedule) > 0:
#             schedule_dict = weekly_schedule[0]
#         elif isinstance(weekly_schedule, dict):
#             schedule_dict = weekly_schedule
#         else:
#             return []
        
#         # Get today's and tomorrow's schedule
#         today_schedule = schedule_dict.get(day_of_week, '')
#         tomorrow_schedule = schedule_dict.get(next_day_of_week, '')
        
#         # Return in the same format as your original function
#         return [{
#             'today_schedule': today_schedule,
#             'tomorrow_schedule': tomorrow_schedule
#         }]
        
#     except Exception as e:
#         print(f"Error extracting schedule from radiator status: {e}")
#         return []
    
# RADIATOR SCHEDULE
def radiator_schedule(room, radiator_id_list):
    
    if radiator_id_list:

        radiator = radiator_id_list[0]
         
        client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD, database=INFLUXDB_GD_DATABASE)
        # Get the day of the week
        day_of_week = datetime.now().strftime("%A").lower()
        next_day_of_week = (datetime.now() + timedelta(days=1)).strftime("%A").lower()
        print('Day of the week', day_of_week)

        query_radiator_schedule = f'''SELECT last("weekly_schedule.{day_of_week}") AS "today_schedule", ("weekly_schedule.{next_day_of_week}") AS "tomorrow_schedule" FROM "gd_data"."autogen"."radiator_data" WHERE "{room}"=\'{radiator}\' '''
        radiator_schedule = client_influx.query(query_radiator_schedule)
        radiator_schedule = list(radiator_schedule.get_points())
        print('Todays radiator schedule', radiator_schedule)

        return radiator_schedule
    
def heating_rate(room, radiator_id_list):
    
    if radiator_id_list:

        radiator = radiator_id_list[0]

        print('The radiator id is', radiator)
         
        client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD, database=INFLUXDB_GD_DATABASE)

        query_heating_rate = f'''SELECT ("time_to_1_degree_hours") AS "heating_rate" FROM "gd_data"."autogen"."heating_analysis" WHERE "room"='{room}' LIMIT 5 '''
        heating_rate = client_influx.query(query_heating_rate)
        heating_rate_df = pd.DataFrame(heating_rate.get_points())

        if not heating_rate_df.empty:

            print('The room average heating rate', heating_rate_df)

            return float(heating_rate_df['heating_rate'].iloc[0])
        
        else: 
            
            heating_rate = DEFAULT_HEATING_RATE

            return heating_rate
    
def cooling_rate(room, radiator_id_list):
    
    if radiator_id_list:

        radiator = radiator_id_list[0]

        print('The radiator id is', radiator)
         
        client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD, database=INFLUXDB_GD_DATABASE)

        query_cooling_rate = f'''SELECT ("cooling_rate") AS "cooling_rate" FROM "gd_data"."autogen"."cooling_sessions" WHERE "{room}"=\'{radiator}\' LIMIT 5 '''
        cooling_rate = client_influx.query(query_cooling_rate)
        cooling_rate_df = pd.DataFrame(cooling_rate.get_points())

        if not cooling_rate_df.empty:

            print('The room average heating rate', cooling_rate_df)

            return float(cooling_rate_df['cooling_rate'].iloc[0])
        
        else: 
            
            cooling_rate = DEFAULT_COOLING_RATE

            return cooling_rate

def parse_schedule(raw_schedule):
    """
    Parse the raw schedule into a structured dictionary for today and tomorrow.

    Parameters:
    - raw_schedule (list): Raw schedule data from the system.

    Returns:
    - dict: Parsed schedule with 'today' and 'tomorrow' keys and their respective schedules.
    """
    # Handle various invalid inputs
    if not raw_schedule or not isinstance(raw_schedule, list):
        return {'today': [], 'tomorrow': []}
    
    schedule = {'today': [], 'tomorrow': []}
    
    for entry in raw_schedule:
        # Skip invalid entries
        if not isinstance(entry, dict):
            continue
        
        # Get schedules with default empty string if key doesn't exist
        today_schedule_str = entry.get('today_schedule', '')
        tomorrow_schedule_str = entry.get('tomorrow_schedule', '')
        
        # Parse today's schedule
        if today_schedule_str:
            try:
                schedule['today'] = [
                    (time_temp.split("/")[0], int(time_temp.split("/")[1])) 
                    for time_temp in today_schedule_str.split()
                    if time_temp and "/" in time_temp
                ]
            except (ValueError, IndexError) as e:
                # Log error or handle invalid format
                print(f"Warning: Invalid format in today_schedule: {today_schedule_str}, error: {e}")
                schedule['today'] = []
        
        # Parse tomorrow's schedule
        if tomorrow_schedule_str:
            try:
                schedule['tomorrow'] = [
                    (time_temp.split("/")[0], int(time_temp.split("/")[1])) 
                    for time_temp in tomorrow_schedule_str.split()
                    if time_temp and "/" in time_temp
                ]
            except (ValueError, IndexError) as e:
                # Log error or handle invalid format
                print(f"Warning: Invalid format in tomorrow_schedule: {tomorrow_schedule_str}, error: {e}")
                schedule['tomorrow'] = []
    
    return schedule

def get_current_and_next_schedule(schedule, current_time):
    """
    Get the current and next target temperatures from the parsed schedule for today and tomorrow.

    Parameters:
    - schedule (dict): Parsed schedule with 'today' and 'tomorrow' schedules.
    - current_time (datetime): Current datetime.

    Returns:
    - tuple: (current_target_temp, next_time, next_target_temp)
    """
    today_schedule = schedule.get("today", [])
    tomorrow_schedule = schedule.get("tomorrow", [])

    # If both schedules are empty, nothing to return
    if not today_schedule and not tomorrow_schedule:
        return None, None, None

    current_temp = None
    next_time = None
    next_temp = None

    # Search today's schedule
    for time_str, temp in today_schedule:
        schedule_time = datetime.strptime(time_str, "%H:%M").replace(
            year=current_time.year, month=current_time.month, day=current_time.day
        )
        if schedule_time <= current_time:
            current_temp = temp
        elif schedule_time > current_time and next_time is None:
            next_time = schedule_time
            next_temp = temp
            break

    # If no "next" found today, look at tomorrow
    if next_time is None and tomorrow_schedule:
        next_time_str, next_temp = tomorrow_schedule[0]
        next_time = datetime.strptime(next_time_str, "%H:%M").replace(
            year=current_time.year, month=current_time.month, day=current_time.day
        ) + timedelta(days=1)

    return current_temp, next_time, next_temp

def heating_decision_manual(current_temp, is_heating, current_target, max_valve_opening):

    temp_difference = current_target - current_temp
        
    # Calculate half of max valve opening rounded to nearest 5.0
    half_max_valve = round(max_valve_opening / 2 / 5.0) * 5.0
    smooth_max_valve = 12.0
    
    if not is_heating and temp_difference > 0:
    
        if temp_difference > 0.3:
            valve_opening = smooth_max_valve
            return "re_heat", valve_opening
        
        else:
            valve_opening = max_valve_opening
            return "no_action", valve_opening
                
    elif is_heating and temp_difference < 0.3:
        valve_opening = smooth_max_valve
        return "controlled_heating", valve_opening
    
    else:
        valve_opening = max_valve_opening
        return "heating", valve_opening

def heating_decision_ai(room, schedule, current_temp, is_heating, current_time, heating_rate, cooling_rate, max_valve_opening):
    """
    Make a decision to start or stop heating based on the schedule.

    Parameters:
    - schedule (dict): Parsed schedule with times and target temperatures.
    - current_temp (float): Current room temperature in degrees.
    - is_heating (bool): Whether the radiator is currently heating.
    - current_time (datetime): Current datetime.
    - heating_rate (float): Heating rate in degrees per hour.

    Returns:
    - str: "pre_heating", "pre_cooling", or "no_action".
    """

    client_influx = connect_or_create_tag(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE, "system_info", room, "room")

    query_system_info = f'SELECT LAST("sub_system_mode") AS "sub_system_mode", ("next_time") AS "next_time" FROM {INFLUXDB_GD_DATABASE}."autogen"."system_info" WHERE "{room}"= \'room\' '
    query_system_info = client_influx.query(query_system_info)
    system_info = list(query_system_info.get_points())
    print('system_info', system_info)

    if not system_info:

        current_target, next_time, next_target = get_current_and_next_schedule(schedule, current_time)

        new_target = current_target
        valve_opening = max_valve_opening
        temp_difference = 0.0
        return "heating", valve_opening, next_time, temp_difference, new_target

    current_sub_system = system_info[0]['sub_system_mode']
    current_next_time = system_info[0]['next_time']

    current_target, next_time, next_target = get_current_and_next_schedule(schedule, current_time)

    if next_time is not None:

        print('next_time before', next_time)

        next_time_full_date = next_time

        next_time = get_hour_minute(next_time)

    half_max_valve = round(max_valve_opening / 2 / 5.0) * 5.0
    smooth_max_valve = 12.0
    

    if None in (current_target, next_time, next_target):
        print("⚠️ Some schedule data is missing.")

        return None, None, None, None, None
    
    else:
        print("✅ Schedule is complete:")

        # Calculate the time needed to reach the next target temperature
        temp_difference = next_target - current_temp - extra_preheating_factor
        temp_difference = float(temp_difference)
        print(temp_difference)

        target_difference = next_target - current_target
        target_difference = float(target_difference)

        current_temp_difference = float(current_target - current_temp)

        if current_next_time in [None, "None"]:

            current_next_time = next_time

            print('current next time', current_next_time)

        else: current_next_time = get_hour_minute(current_next_time)

        if current_sub_system == 'pre_heating' and is_heating and temp_difference > 0 and current_next_time == next_time:


            valve_opening = half_max_valve


            return "pre_heating", valve_opening, next_time, target_difference, next_target
        
        elif current_sub_system == 'pre_cooling' and not is_heating and temp_difference < 0 and current_next_time == next_time:

            valve_opening = max_valve_opening
            return "pre_cooling", valve_opening, next_time, target_difference, next_target
        
        elif not is_heating and temp_difference > 0 and heating_rate is not None:
        
            time_needed_hours = temp_difference * heating_rate
            print(time_needed_hours)
            
            if time_needed_hours > 0.05:

                # Determine when to start heating for the next schedule
                if next_time:
                    print('next_time', next_time_full_date)
                    latest_start_time = next_time_full_date - timedelta(hours=time_needed_hours) + timedelta(minutes=10)
                    print(latest_start_time)
                    # add limit ??
                    if current_time >= latest_start_time:
                        new_target = next_target
                            
                        valve_opening = half_max_valve
                        
                        return "pre_heating", valve_opening, next_time, target_difference, new_target
        
                    
                    elif current_temp < current_target:
        
                        if current_temp < current_target - 0.3:
                            new_target = current_target

                            valve_opening = smooth_max_valve

                            return "re_heat", valve_opening, next_time, temp_difference, new_target
                        
                        else:
                            new_target = current_target
                            valve_opening = max_valve_opening
                            return "idle", valve_opening, next_time, temp_difference, new_target
                        
                    else:
                        new_target = current_target
                        valve_opening = max_valve_opening
                        return "idle", valve_opening, next_time, temp_difference, new_target
                        
            else:
                new_target = current_target
                valve_opening = max_valve_opening
                return "idle", valve_opening, next_time, temp_difference, new_target

        
        elif is_heating and temp_difference < 0:

            time_needed_hours = abs(temp_difference / cooling_rate)

            print('time_needed_hours', time_needed_hours)
        
            # Determine when to stop heating for the next schedule
            if next_time and time_needed_hours > 0.0:
                print('next_time', next_time_full_date)

                time_until_next = (next_time_full_date - current_time).total_seconds() / 3600.0
                print('time_until_next', time_until_next)

                latest_start_time = next_time_full_date - timedelta(hours=time_needed_hours)
                print(latest_start_time)

                # add limit ??
                #if current_time >= latest_start_time:
                if time_until_next <= 1.0:

                    new_target = next_target
                    valve_opening = max_valve_opening
                    return "pre_cooling", valve_opening, next_time, target_difference, new_target
                
                elif is_heating and current_temp_difference < 0.3:
                    new_target = current_target

                    valve_opening = smooth_max_valve

                    return "controlled_heating", valve_opening, next_time, temp_difference, new_target
                
                else:
                    new_target = current_target
                    valve_opening = max_valve_opening
                    return "heating", valve_opening, next_time, temp_difference, new_target

            elif is_heating and current_temp_difference < 0.3:
                    new_target = current_target
                
                    valve_opening = smooth_max_valve
                
                    return "controlled_heating", valve_opening, next_time, temp_difference, new_target
            
            else:
                new_target = current_target
                valve_opening = max_valve_opening
                return "heating", valve_opening, next_time, temp_difference, new_target
                
        # If the current temperature is above the target and the heater is on, turn it off
        elif current_temp < current_target and not is_heating:

            if current_temp < current_target - 0.3:
                new_target = current_target
                
                valve_opening = smooth_max_valve
                
                return "re_heat", valve_opening, next_time, temp_difference, new_target
            
            else:
                new_target = current_target
                valve_opening = max_valve_opening
                return "idle", valve_opening, next_time, temp_difference, new_target
        
        elif is_heating:

            if current_temp_difference < 0.8:
                    new_target = current_target

                    valve_opening = smooth_max_valve
                    
                    return "controlled_heating", valve_opening, next_time, temp_difference, new_target
            
            else:
                new_target = current_target
                valve_opening = max_valve_opening
                return "heating", valve_opening, next_time, temp_difference, new_target
        
        # If the current temperature is already above the target and the heater is off, no need to heat    
        elif not is_heating:

            new_target = current_target
            valve_opening = max_valve_opening
            return "idle", valve_opening, next_time, temp_difference, new_target
            
        else:
            new_target = current_target
            valve_opening = max_valve_opening
            return "heating", valve_opening, next_time, temp_difference, new_target

def local_temperature_calibration(room, sub_system_mode, system_mode, valve_opening, temp_difference, new_target, mode_change, secondary_thu_offset):

    sensor_table = sensors_table(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)

    room_temp_aq = None
    added_offset = 0.0
    overshoot_offset = 0.0
    occupancy_offset = 0.0
    
    print(room)
    radiator_sensor_id = [sensor['sensor_id'] for sensor in sensor_table if sensor['room_id'] == room and sensor['sensor_type'] == "Zigbee thermostatic radiator valve" ]
    print(radiator_sensor_id)

    temp_sensor_id = [sensor['sensor_id'] for sensor in sensor_table if sensor['room_id'] == room and (sensor['sensor_type'] == "Smart air house keeper" or sensor['sensor_type'] == "Temperature and humidity sensor with screen" or sensor['sensor_type'] == "Temperature and humidity sensor") ]
    
    print(temp_sensor_id)

    occupancy_id = [ sensor['sensor_id'] for sensor in sensor_table if sensor['room_id'] == room and (sensor['sensor_type'] == "Zigbee occupancy sensor" or sensor['sensor_type'] == "Motion sensor" or sensor['sensor_type'] == "PIR 24Ghz human presence sensor")]

    occupancy_type = [sensor['sensor_type'] for sensor in sensor_table 
                        if sensor['room_id'] == room
                        and (sensor['sensor_type'] == "Zigbee occupancy sensor"
                            or sensor['sensor_type'] == "Motion sensor" 
                                or sensor['sensor_type'] == "PIR 24Ghz human presence sensor")]      

    print('occupancy type', occupancy_type)
    
    client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD, database=INFLUXDB_DATABASE)

    if temp_sensor_id:

        entity_id = temp_sensor_id[0] + '_temperature'
        query_room_temp = f'SELECT LAST("value") FROM {INFLUXDB_DATABASE}."autogen"."°C" WHERE "entity_id"=\'{entity_id}\''
        room_temp = client_influx.query(query_room_temp)
        room_temp_aq = list(room_temp.get_points())
    
    status = []

    if room_temp_aq:

        for radiator in radiator_sensor_id:

            added_offset = 0.0
            overshoot_offset = 0.0
            occupancy_offset = 0.0

            # Topics for getting the local tempearture from the smar thermostat sensor state
            REQUEST_TOPIC = 'zigbee2mqtt/' + radiator + '/get'
            STATE_TOPIC = 'zigbee2mqtt/' + radiator  # The state will be published here after the request
            print('TOPICS', REQUEST_TOPIC, STATE_TOPIC)

            # Payload for the state request (empty, Zigbee2MQTT may accept an empty payload for 'get' requests)
            REQUEST_PAYLOAD = {"local_temperature" : "",
                                "local_temperature_calibration": "",
                                "occupied_heating_setpoint" : "",
                                "valve_opening_degree" : "",
                                "system_mode": ""}

            # Timeout settings
            TIMEOUT_SECONDS = 3  # Maximum time to wait for the response in seconds
            start_time = time.time()  # Record the time the request is sent

            # Global variable to store the retrieved temperature value
            temperature_received = False

            # Shared state between callbacks
            shared_state = {
                "local_temperature": None,
                "temperature_received": False,
                "local_temperature_calibration": None,
                "valve_opening_degree": None,
                "system_mode": None
           }

            # Create MQTT client instance
            mqtt_client = mqtt.Client()

            # Set username and password if needed
            mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

            # Set the connection and message callbacks
            mqtt_client.on_connect = lambda client, userdata, flags, rc: on_connect(client, userdata, flags, rc, STATE_TOPIC, REQUEST_TOPIC, REQUEST_PAYLOAD)
            mqtt_client.on_message = lambda client, userdata, msg: on_message(client, userdata, msg, shared_state)

            # Connect to the MQTT broker
            mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)

            # Start the MQTT loop to handle incoming messages
            mqtt_client.loop_start()

            # Wait for the local temperature to be retrieved
            while not temperature_received and (time.time() - start_time) < TIMEOUT_SECONDS:
                time.sleep(0.1)  # Avoid tight loop, sleep for a short while (100ms)


            # Stop the loop and disconnect if the timeout is reached
            if shared_state['local_temperature'] is None:
                print(f"No local_temperature received within {TIMEOUT_SECONDS} seconds.")
            else:
                print(f"The retrieved temperature is: {shared_state['local_temperature']}")

            
            if shared_state['local_temperature'] is not None:
                radiator_temp = shared_state['local_temperature']
                current_temperature_offset = shared_state['local_temperature_calibration']
                current_valve_opening = shared_state['valve_opening_degree']
                current_heating_setpoint = shared_state['occupied_heating_setpoint']
                print('current_temperature_offset', current_temperature_offset)

                if current_temperature_offset is None:
                    current_temperature_offset = 0.0

                mqtt_client.loop_stop()
                mqtt_client.disconnect()

                if current_valve_opening is not None and (
                    (system_mode == 'MANUAL' and shared_state['system_mode'] == 'heat') or 
                    (system_mode == 'AI' and shared_state['system_mode'] == 'auto') or
                    shared_state['system_mode'] == 'off' or
                    mode_change or 
                    (len(radiator_sensor_id) > 1) #disable zonal heating
                    ):
                    
                    status.append('True')

                    if occupancy_id:

                        client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD, database=INFLUXDB_DATABASE)

                        entity_id = temp_sensor_id[0] + '_temperature'
                        query_presence_activation = f'SELECT last("value") AS "presence_activation_state" FROM "ga_homeassistant_db"."autogen"."state" WHERE "entity_id"=\'presence_activation_{room}\''
                        presence_activation = client_influx.query(query_presence_activation)
                        presence_activation = list(presence_activation.get_points())

                        if (presence_activation and presence_activation[0].get('presence_activation_state') == 1.0) or not presence_activation:

                            occupancy_quality = occupancy_data_quality(occupancy_id, occupancy_type)

                            if occupancy_quality:

                                current_occupancy = occupancy_data(occupancy_id, occupancy_type, 30, '30m')

                                print('current_occupancy', current_occupancy)

                                if current_occupancy >= 0.0:

                                    if current_occupancy > 0.05:

                                        occupancy_offset = 0

                                    elif current_occupancy <= 0.05:

                                        if sub_system_mode == 'pre_heating':
                                            
                                            occupancy_offset = 0

                                        else:

                                            if current_heating_setpoint > 16.0:

                                                occupancy_offset = (current_heating_setpoint*0.1)
                                                occupancy_offset = round(occupancy_offset, 1)

                                            else: occupancy_offset = 0

                                else: occupancy_offset = 0

                        else: occupancy_offset = 0        

                    else:

                        print('NO OCCUPANCY SENSOR AVAILABLE')
                        occupancy_offset = 0
                        
                    print('occupancy_offset', occupancy_offset)

                    if sub_system_mode == 're_heat':
                    
                        added_offset = -0.5

                    elif sub_system_mode == 'pre_heating': 
                        
                        added_offset = temp_difference *-1

                    elif sub_system_mode == 'pre_cooling':

                        added_offset = temp_difference *-1
                    
                    else: added_offset = 0.0
                    
                    raw_temperature_offset = (room_temp_aq[0]['last'] - radiator_temp) + current_temperature_offset
                    
                    if raw_temperature_offset < -2.5:
                    
                        overshoot_offset = 0.0

                    else: overshoot_offset = 0
                    
                    print('VALVE OPENING', valve_opening)

                    if system_mode == 'MANUAL':

                        mode = 'heat'

                        temperature_offset = (room_temp_aq[0]['last'] - radiator_temp) + current_temperature_offset + added_offset - early_stop
                        temperature_offset = round(temperature_offset, 1)
                        print(temperature_offset)

                        if temperature_offset < -12.6:
                            temperature_offset = -12.6
                            print('LIMITED TEMPERATURE OFFSET TO -12.6°C')

                        elif temperature_offset > 12.6:
                            temperature_offset = 12.6
                            print('LIMITED TEMPERATURE OFFSET TO 12.6°C')

                        #if temperature_offset != current_temperature_offset or current_valve_opening != valve_opening:

                        MQTT_TOPIC = 'zigbee2mqtt/' + radiator + '/set'

                        PAYLOAD = {"local_temperature_calibration": temperature_offset,
                                    "valve_opening_degree": valve_opening,
                                    "system_mode" : mode,
                                    "occupied_heating_setpoint" : new_target
                                    }      

                        # Create MQTT client instance
                        mqtt_client = mqtt.Client()

                        # Set username and password if needed
                        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

                        # Set the connection and message callbacks
                        mqtt_client.on_connect = lambda mqtt_client, userdata, flags, rc: on_connect_radiator(mqtt_client, userdata, flags, rc, MQTT_TOPIC, PAYLOAD)
                        mqtt_client.on_message = lambda mqtt_client, userdata, msg: on_message_radiator(mqtt_client, userdata, msg)

                        # Connect to the MQTT broker
                        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)

                        # Start the MQTT loop to handle incoming messages
                        mqtt_client.loop_start()

                        time.sleep(2)  # Avoid tight loop

                        mqtt_client.loop_stop()
                        mqtt_client.disconnect()

                    elif shared_state['system_mode'] == 'off':
                        
                        temperature_offset = (room_temp_aq[0]['last'] - radiator_temp) + current_temperature_offset + secondary_thu_offset
                        temperature_offset = round(temperature_offset, 1)
                        print(temperature_offset)

                        if temperature_offset < -12.6:
                            temperature_offset = -12.6
                            print('LIMITED TEMPERATURE OFFSET TO -12.6°C')

                        elif temperature_offset > 12.6:
                            temperature_offset = 12.6
                            print('LIMITED TEMPERATURE OFFSET TO 12.6°C')

                        #if temperature_offset != current_temperature_offset or current_valve_opening != valve_opening:

                        MQTT_TOPIC = 'zigbee2mqtt/' + radiator + '/set'

                        PAYLOAD = {"local_temperature_calibration": temperature_offset}      

                        # Create MQTT client instance
                        mqtt_client = mqtt.Client()

                        # Set username and password if needed
                        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

                        # Set the connection and message callbacks
                        mqtt_client.on_connect = lambda mqtt_client, userdata, flags, rc: on_connect_radiator(mqtt_client, userdata, flags, rc, MQTT_TOPIC, PAYLOAD)
                        mqtt_client.on_message = lambda mqtt_client, userdata, msg: on_message_radiator(mqtt_client, userdata, msg)

                        # Connect to the MQTT broker
                        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)

                        # Start the MQTT loop to handle incoming messages
                        mqtt_client.loop_start()

                        time.sleep(2)  # Avoid tight loop

                        mqtt_client.loop_stop()
                        mqtt_client.disconnect()
                        
                    elif system_mode == 'AI':
                        
                        mode = 'auto'

                        temperature_offset = (room_temp_aq[0]['last'] - radiator_temp) + current_temperature_offset + added_offset + overshoot_offset + secondary_thu_offset + extra_factor + occupancy_offset - early_stop_ai
                        temperature_offset = round(temperature_offset, 1)
                        print(temperature_offset)

                        if temperature_offset < -12.6:
                            temperature_offset = -12.6
                            print('LIMITED TEMPERATURE OFFSET TO -12.6°C')

                        elif temperature_offset > 12.6:
                            temperature_offset = 12.6
                            print('LIMITED TEMPERATURE OFFSET TO 12.6°C')

                        #if temperature_offset != current_temperature_offset or current_valve_opening != valve_opening:

                        MQTT_TOPIC = 'zigbee2mqtt/' + radiator + '/set'

                        PAYLOAD = {"local_temperature_calibration": temperature_offset,
                                    "valve_opening_degree": valve_opening,
                                    "system_mode" : mode}      

                        # Create MQTT client instance
                        mqtt_client = mqtt.Client()

                        # Set username and password if needed
                        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

                        # Set the connection and message callbacks
                        mqtt_client.on_connect = lambda mqtt_client, userdata, flags, rc: on_connect_radiator(mqtt_client, userdata, flags, rc, MQTT_TOPIC, PAYLOAD)
                        mqtt_client.on_message = lambda mqtt_client, userdata, msg: on_message_radiator(mqtt_client, userdata, msg)

                        # Connect to the MQTT broker
                        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)

                        # Start the MQTT loop to handle incoming messages
                        mqtt_client.loop_start()

                        time.sleep(2)  # Avoid tight loop

                        mqtt_client.loop_stop()
                        mqtt_client.disconnect()
                        
                    else:
                        
                        temperature_offset = (room_temp_aq[0]['last'] - radiator_temp) + current_temperature_offset + overshoot_offset + secondary_thu_offset + extra_factor + occupancy_offset
                        temperature_offset = round(temperature_offset, 1)
                        print(temperature_offset)

                        #if temperature_offset != current_temperature_offset or current_valve_opening != valve_opening:

                        MQTT_TOPIC = 'zigbee2mqtt/' + radiator + '/set'

                        PAYLOAD = {"local_temperature_calibration": temperature_offset}      

                        # Create MQTT client instance
                        mqtt_client = mqtt.Client()

                        # Set username and password if needed
                        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

                        # Set the connection and message callbacks
                        mqtt_client.on_connect = lambda mqtt_client, userdata, flags, rc: on_connect_radiator(mqtt_client, userdata, flags, rc, MQTT_TOPIC, PAYLOAD)
                        mqtt_client.on_message = lambda mqtt_client, userdata, msg: on_message_radiator(mqtt_client, userdata, msg)

                        # Connect to the MQTT broker
                        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)

                        # Start the MQTT loop to handle incoming messages
                        mqtt_client.loop_start()

                        time.sleep(2)  # Avoid tight loop

                        mqtt_client.loop_stop()
                        mqtt_client.disconnect()

                else: status.append('False')
            
            else: status.append('False')
            

    occupancy_offset = 0 if occupancy_offset is None else occupancy_offset
    
    if status and all(status): 
        
        return True, occupancy_offset
    
    else: return False, occupancy_offset

        #else: continue

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

def connect_or_create_tag(host, port, user, password, dbname, measurement_name, room_id, tag):
    client = influxdb.InfluxDBClient(host, port, user, password)
    if database_exists(client, dbname):
        print(f"Database '{dbname}' already exists.")
        
    else:
        client.create_database(dbname)
        print(f"Database '{dbname}' created successfully.")

    client = influxdb.InfluxDBClient(host, port, user, password, dbname)

    if measurement_exists(client, dbname, measurement_name):
        print(f"Measurement '{measurement_name}' already exists in database '{dbname}'.")

        if tag_value_exists(client, dbname, measurement_name, f"{room_id}", tag):
            print(f"Tag '{tag}' exists in measurement '{measurement_name}'.")
            return influxdb.InfluxDBClient(host, port, user, password, dbname)
        
        else:
            
            print(f"Tag '{room_id}_{tag}' does not exist. Creating it and adding data...")
        
            # Data to be added for each tag
            data = [
            {"system_mode": "AI", "sub_system_mode": "heating", "system_setpoint": 20.0, "next_time" : "2025-01-01 00:00:00"}]

            # Prepare the data points for InfluxDB
            json_body = [
                {
                    "measurement": measurement_name,
                    "tags": {
                        f"{room_id}": tag 
                    },
                    "time": datetime.now(timezone.utc).isoformat(),  # Use the current time
                    "fields": {
                        "system_mode": data[0]["system_mode"],
                        "sub_system_mode": data[0]["sub_system_mode"],
                        "system_setpoint": data[0]["system_setpoint"],
                        "next_time": data[0]["next_time"]
                    }
                }
            ]
            
            # Write the data points to InfluxDB
            client.write_points(json_body)
            print(f"Measurement '{measurement_name}' created successfully with data for tags: {tag}.")
            return influxdb.InfluxDBClient(host, port, user, password, dbname)

    
    else: 
        
        print(f"Measurement '{measurement_name}' does not exist. Creating it and adding default data...")

            # Data to be added for each tag
        data = [
            {"system_mode": "AI", "sub_system_mode": "heating", "system_setpoint": 20.0, "next_time" : "2025-02-10 22:00:00"}]

        # Prepare the data points for InfluxDB
        json_body = [
            {
                "measurement": measurement_name,
                "tags": {
                    room_id: tag
                },
                "time": datetime.now(timezone.utc).isoformat(),  # Use the current time
                "fields": {
                    "system_mode": data[0]["system_mode"],
                    "sub_system_mode": data[0]["sub_system_mode"],
                    "system_setpoint": data[0]["system_setpoint"],
                    "next_time": data[0]["next_time"]
                }
            }
        ]

        # Write the data points to InfluxDB
        client.write_points(json_body)
        print(f"Measurement '{measurement_name}' created successfully with data for tags: {tag}.")

    return influxdb.InfluxDBClient(host, port, user, password, dbname)

def measurement_exists(client, dbname, measurement_name):
    # Query to check if the measurement exists
    query = f'SHOW MEASUREMENTS ON "{dbname}"'
    result = client.query(query)
    
    # Extract the list of measurements from the result
    measurements = list(result.get_points())
    
    # Check if the measurement exists in the list
    return any(measurement['name'] == measurement_name for measurement in measurements)

def tag_value_exists(client, dbname, measurement, tag_key, tag_value):
    query = f'SELECT * FROM "{measurement}" WHERE "{tag_key}" = \'{tag_value}\' LIMIT 1'
    result = client.query(query, database=dbname)

    return len(list(result.get_points())) > 0  # True if at least one matching point exists

# Callback when the client connects to the broker
def on_connect_radiator(mqtt_client, userdata, flags, rc, request_topic, request_payload):
    print(f"Connected to MQTT broker with result code {rc}")

    # Publish the request to get the current state
    mqtt_client.publish(request_topic, json.dumps(request_payload))
    print(f"Published state request to {request_topic}")

# Callback when a message is received from the broker
def on_message_radiator(client, userdata, msg):
    print(f"Received message from {msg.topic}: {msg.payload.decode()}")

def radiator_status(radiator):

    # Topics for getting the local tempearture from the smar thermostat sensor state
    REQUEST_TOPIC = 'zigbee2mqtt/' + radiator + '/get'
    STATE_TOPIC = 'zigbee2mqtt/' + radiator  # The state will be published here after the request
    print('TOPICS', REQUEST_TOPIC, STATE_TOPIC)

    # Payload for the state request (empty, Zigbee2MQTT may accept an empty payload for 'get' requests)
    REQUEST_PAYLOAD = {
        "occupied_heating_setpoint": {},
        "open_window": {},
        "running_state": {},
        "system_mode": {},
        "valve_opening_degree": {},
        "weekly_schedule": {}
    }

    # Timeout settings
    TIMEOUT_SECONDS = 3  # Maximum time to wait for the response in seconds
    start_time = datetime.now()  # Record the time the request is sent

    # Global variable to store the retrieved temperature value
    temperature_received = False

    # Shared state between callbacks
    shared_state = {
        "system_mode": None,
        "data_received": False,
        "occupied_heating_setpoint": None,
        "valve_opening_degree": None,
        "open_window": None,
        "running_state": None,
        "weekly_schedule": None
    }

    # Create MQTT client instance
    mqtt_client = mqtt.Client()

    # Set username and password if needed
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    # Set the connection and message callbacks
    mqtt_client.on_connect = lambda client, userdata, flags, rc: on_connect(
        client, userdata, flags, rc, STATE_TOPIC, REQUEST_TOPIC,
        REQUEST_PAYLOAD)
    mqtt_client.on_message = lambda client, userdata, msg: on_message(
        client, userdata, msg, shared_state)

    # Connect to the MQTT broker
    mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)

    # Start the MQTT loop to handle incoming messages
    mqtt_client.loop_start()

    # Wait for the local temperature to be retrieved
    while not temperature_received and (datetime.now() -
                                        start_time) < timedelta(seconds=TIMEOUT_SECONDS):
        time.sleep(0.1)  # Avoid tight loop, sleep for a short while (100ms)

    # Stop the loop and disconnect if the timeout is reached
    if shared_state['system_mode'] is None:
        print(f"No system_mode received within {TIMEOUT_SECONDS} seconds.")
    else:
        print(f"The retrieved system mode is: {shared_state['system_mode']}")

    mqtt_client.loop_stop()
    mqtt_client.disconnect()

    return shared_state

def parse_influx_time(nanoseconds):
    """Convert InfluxDB nanosecond timestamp to datetime"""
    seconds = nanoseconds / 1e9  # Convert nanoseconds to seconds
    return datetime.fromtimestamp(seconds).astimezone(LOCAL_TZ)

def secondary_thu(room):
    """Synchronous version without asyncio"""
    # Configuration
    current_time = datetime.now(LOCAL_TZ)
    current_hour = current_time.replace(minute=0, second=0, microsecond=0)
    current_hour_str = current_time.hour
    print(current_hour_str)

    # Get time range for today
    today = datetime.now(LOCAL_TZ).date()
    current_month = today.month
    current_day = today.day
    start_of_day = LOCAL_TZ.localize(datetime.combine(today, datetime.min.time()))
    end_of_day = LOCAL_TZ.localize(datetime.combine(today, datetime.max.time()))

    client = influxdb.InfluxDBClient(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)

    try:
        # Query 1: Current temperature (most recent)
        current_query = f'''
        SELECT ("temperature") AS "current_temp" 
        FROM "current_weather" 
        WHERE time >= '{start_of_day.isoformat()}' 
        AND time <= '{end_of_day.isoformat()}'
        AND  "hour" = '{current_hour_str}' 
        AND "data_type" = 'hourly'
        '''
        
        # Query 2: Max temperature today
        max_query = f'''
        SELECT max("temperature") AS "max_temp" 
        FROM "current_weather" 
        WHERE time >= '{start_of_day.isoformat()}' 
        AND time <= '{end_of_day.isoformat()}'
        AND "data_type" = 'hourly'
        '''
        
        # New daylight query
        daylight_query = f'''
        SELECT "sunrise" AS "mean_sunrise", "sunset" AS "mean_sunset" 
        FROM "gd_data"."autogen"."daylight" 
        WHERE "month" = {current_month} AND "day" = {current_day}
        LIMIT 1
        '''

        # Room transfer coefficient query
        trans_coef_query = f'''
        SELECT last("transfer_coefficient") AS "transfer_coefficient" 
        FROM "gd_data"."autogen"."transfer_coefficient" 
        WHERE "room"= '{room}'
        '''
        
        # Execute all queries and convert generators to lists
        current_res = list(client.query(current_query))
        max_res = list(client.query(max_query))
        daylight_res = list(client.query(daylight_query))
        trans_coef_res = list(client.query(trans_coef_query))

        # Process temperature results - adjust indexing based on actual structure
        # The structure might be different, so let's debug first
        print("Current result structure:", current_res)
        print("Max result structure:", max_res)
        print("Daylight result structure:", daylight_res)
        print("Trans coef result structure:", trans_coef_res)
        
        # Try different approaches to access the data
        if current_res and len(current_res) > 0:
            # Method 1: If it's a list of ResultSets
            if hasattr(current_res[0], 'get_points'):
                current_points = list(current_res[0].get_points())
                current_temp = current_points[0]['current_temp'] if current_points else None
            # Method 2: If it's a direct dictionary-like structure
            elif isinstance(current_res[0], dict):
                current_temp = current_res[0].get('current_temp')
            else:
                # Try to access directly
                current_temp = current_res[0][0] if len(current_res[0]) > 0 else None
        else:
            current_temp = None

        if max_res and len(max_res) > 0:
            if hasattr(max_res[0], 'get_points'):
                max_points = list(max_res[0].get_points())
                max_temp = max_points[0]['max_temp'] if max_points else None
                max_time_ns = max_points[0]['time'] if max_points else None
            elif isinstance(max_res[0], dict):
                max_temp = max_res[0].get('max_temp')
                max_time_ns = max_res[0].get('time')
            else:
                max_temp = max_res[0][0] if len(max_res[0]) > 0 else None
                max_time_ns = max_res[0][1] if len(max_res[0]) > 1 else None
        else:
            max_temp = None
            max_time_ns = None

        if daylight_res and len(daylight_res) > 0:
            if hasattr(daylight_res[0], 'get_points'):
                daylight_points = list(daylight_res[0].get_points())
                sunrise_str = daylight_points[0]['mean_sunrise'] if daylight_points else None
                sunset_str = daylight_points[0]['mean_sunset'] if daylight_points else None
            elif isinstance(daylight_res[0], dict):
                sunrise_str = daylight_res[0].get('mean_sunrise')
                sunset_str = daylight_res[0].get('mean_sunset')
            else:
                sunrise_str = daylight_res[0][1] if len(daylight_res[0]) > 1 else None
                sunset_str = daylight_res[0][2] if len(daylight_res[0]) > 2 else None
        else:
            sunrise_str = None
            sunset_str = None

        if trans_coef_res and len(trans_coef_res) > 0:
            if hasattr(trans_coef_res[0], 'get_points'):
                trans_coef_points = list(trans_coef_res[0].get_points())
                trans_coef = trans_coef_points[0]['transfer_coefficient'] if trans_coef_points else None
            elif isinstance(trans_coef_res[0], dict):
                trans_coef = trans_coef_res[0].get('transfer_coefficient')
            else:
                trans_coef = trans_coef_res[0][0] if len(trans_coef_res[0]) > 0 else None
        else:
            trans_coef = None

        # Check if we got all required data
        if None in [current_temp, max_temp, max_time_ns, sunrise_str, sunset_str, trans_coef]:
            print(f"Missing data: current_temp={current_temp}, max_temp={max_temp}, max_time={max_time_ns}, sunrise={sunrise_str}, sunset={sunset_str}, trans_coef={trans_coef}")
            return None

        # Parse max time
        max_time = parse_influx_time(max_time_ns)
        
        # Convert sunrise/sunset to datetime objects
        sunrise_time = datetime.strptime(sunrise_str, "%H:%M:%S").time()
        sunset_time = datetime.strptime(sunset_str, "%H:%M:%S").time()

        # Create full datetime objects with today's date
        sunrise_dt = LOCAL_TZ.localize(datetime.combine(today, sunrise_time))
        sunset_dt = LOCAL_TZ.localize(datetime.combine(today, sunset_time))
        
        # Calculate daylight markers (+1 hour after sunrise, -1 hour before sunset)
        sunrise_plus_1 = sunrise_dt + timedelta(hours=1)
        sunset_minus_1 = sunset_dt - timedelta(hours=1)
        
        # Check daylight conditions
        daylight_status = {
            "sunrise": sunrise_dt.strftime("%H:%M"),
            "sunset": sunset_dt.strftime("%H:%M"),
            "is_morning": current_time > sunrise_plus_1,
            "is_evening": current_time > sunset_minus_1,
            "is_daylight": sunrise_plus_1 <= current_time <= sunset_minus_1
        }

        # Determine if max temp has passed
        max_passed = current_time > max_time
        temp_diff = max_temp - current_temp if not max_passed else 0

        return {
            "current_temp": round(current_temp, 1),
            "current_hour": current_hour_str,
            "max_temp": round(max_temp, 1),
            "max_time": max_time.isoformat(),
            "max_passed": max_passed,
            "temp_diff": round(temp_diff, 1),
            "daylight": daylight_status,
            "room_trans_coef": trans_coef,
            "status": (
                f"Now ({current_hour.hour:02d}:00): {current_temp}°C | "
                f"Max: {max_temp}°C at {max_time.hour:02d}:00 "
                f"({'passed' if max_passed else f'+{temp_diff:.1f}°C'}) | "
                f"Daylight: {sunrise_dt.strftime('%H:%M')} to {sunset_dt.strftime('%H:%M')}"
            )
        }

    except Exception as e:
        print(f"Error querying temperature data: {str(e)}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        client.close()  # Close the client connection

def get_hour_minute(time_input):
    """
    If input is a datetime object or a string in 'YYYY-MM-DD HH:MM:SS' format,
    return only HH:MM as string. Otherwise return as-is.
    """
    # Handle datetime object
    if isinstance(time_input, datetime):
        return time_input.strftime("%H:%M")
    
    # Handle string
    elif isinstance(time_input, str):
        try:
            dt = datetime.strptime(time_input, "%Y-%m-%d %H:%M:%S")
            return dt.strftime("%H:%M")
        except ValueError:
            return time_input  # Return original string if format doesn't match
    
    # Handle other types (int, time object, etc.)
    else:
        # Try to convert to string and handle it
        try:
            return get_hour_minute(str(time_input))
        except:
            return time_input  # Return original if conversion fails
    
def room_mode(room, room_radiators, current_temp, heating_rate, cooling_rate):
    # Initialize all return variables at the start
    radiators_mode = []
    mode_change = []
    end_manual = 'unknown'  # Default value
    mode_change_to = None
    last_manual_mode_time_str = 'mehr als 1 Tag'  # Default value
    duration_of_last_manual_str = '0 Minuten'  # Default value
    max_valve_opening = 100.0  # Default value
    current_target = None
    local_tz = pytz.timezone('Europe/Berlin')
    is_heating = []

    for radiator in room_radiators:
        client_influx = connect_or_create_tag(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE, "system_info", room, radiator)

        query_system_info = f'SELECT LAST("system_setpoint") AS "system_setpoint", ("system_mode") AS "system_mode", ("next_time") AS "next_time", ("sub_system_mode") AS "sub_system_mode" FROM {INFLUXDB_GD_DATABASE}."autogen"."system_info" WHERE "{room}"= \'{radiator}\' '
        query_system_info = client_influx.query(query_system_info)
        system_info = list(query_system_info.get_points())
        print('system_info', system_info)

        query_valve_opening_building = f'SELECT LAST("balanced_valve_opening_building") AS "max_valve_opening" FROM {INFLUXDB_GD_DATABASE}."autogen"."building_hydraulic_balancing" WHERE "room"= \'{room}\' '
        query_valve_opening_building = client_influx.query(query_valve_opening_building)
        max_valve_opening_df = pd.DataFrame(query_valve_opening_building.get_points())
        print('max_valve_opening for building', max_valve_opening_df)

        if max_valve_opening_df.empty:
            query_valve_opening = f'SELECT LAST("balanced_valve_opening") AS "max_valve_opening" FROM {INFLUXDB_GD_DATABASE}."autogen"."hydraulic_balancing" WHERE "room"= \'{room}\' '
            query_valve_opening = client_influx.query(query_valve_opening)
            max_valve_opening_df = pd.DataFrame(query_valve_opening.get_points())
            print('max_valve_opening apartment', max_valve_opening_df)
            
            if max_valve_opening_df.empty:
                max_valve_opening = 25.0
            else: 
                max_valve_opening = max_valve_opening_df['max_valve_opening'].iloc[0]
            
        else: 
            max_valve_opening = max_valve_opening_df['max_valve_opening'].iloc[0]

        if max_valve_opening > 20.0:
            max_valve_opening = 20.0
        if max_valve_opening < 12.0:
            max_valve_opening = 12.0

        max_valve_opening = round(max_valve_opening / 10) * 10
        client_influx.close()
        
        data = radiator_status(radiator)

        if data:
            current_setpoint = data['occupied_heating_setpoint']
            current_target = current_setpoint  # Store for return
            
            if current_setpoint and system_info:
                print('current_setpoint', current_setpoint)
                print('system_info[0][system_setpoint]', system_info[0]['system_setpoint'])
                print('running state', data['running_state'])
                print('system_mode', system_info[0]['system_mode'])
                
                # Get thermostat state and database state
                thermostat_mode = data['system_mode']  # 'off', 'heat', or 'auto'
                db_system_mode = system_info[0]['system_mode'] if system_info else None
                db_setpoint = float(system_info[0]['system_setpoint']) if system_info and system_info[0]['system_setpoint'] else None
                current_setpoint_float = float(current_setpoint)
                is_heating_radiator = True if data['running_state'] == 'heat' else False  # Radiator running state
                is_heating.append(is_heating_radiator)
                
                # Clear state machine logic - thermostat state takes priority
                if thermostat_mode == 'off':
                    # Thermostat is OFF - highest priority
                    radiators_mode.append("off")
                    system_mode = "off"
                    sub_system_mode = "idle"
                    mode_change.append(db_system_mode != "off")
                    print(f"Mode: OFF (thermostat says off)")
                    
                elif thermostat_mode == 'heat':
                    # Thermostat is in MANUAL heating mode

                    query_setpoint_ai = f'SELECT LAST("system_setpoint") AS "system_setpoint" FROM {INFLUXDB_GD_DATABASE}."autogen"."system_info" WHERE "{room}"= \'{radiator}\' AND "system_mode" = \'AI\' '
                    query_manual_start = client_influx.query(query_setpoint_ai)
                    data_setpoint_ai = list(query_manual_start.get_points())
                    print('data_setpoint_ai', data_setpoint_ai)

                    if current_setpoint_float and data_setpoint_ai[0]['system_setpoint']: 
                        #temperature_threshold = current_setpoint_float - 0.5

                        print('ai time', data_setpoint_ai[0]['time'])

                        room_temp_ai = room_temperature_last_ai(room, data_setpoint_ai[0]['time'])

                        delta_setpoint = current_setpoint_float - data_setpoint_ai[0]['system_setpoint'] if data_setpoint_ai else None

                        delta_temperature = current_setpoint_float - room_temp_ai if room_temp_ai is not None else None

                        print(f'Delta setpoint: {delta_setpoint}°C')

                        print(f'Delta temperature: {delta_temperature}°C')

                        delta_temperature = round(delta_temperature, 2)

                        if delta_setpoint is not None and delta_setpoint >= 0:

                            if delta_temperature is not None and delta_temperature >= 0:

                                print(f'Heating rate: {heating_rate} hour')

                                duration_of_manual_mode = (delta_temperature * heating_rate) + 1.0 if heating_rate > 0 else float(1.0)

                            else:
                                duration_of_manual_mode = 1.0  # Default to 1 hour if current temp is above setpoint

                            if duration_of_manual_mode > 3.0:
                                duration_of_manual_mode = 3.0  # Cap at 3 hours

                        elif delta_setpoint is not None and delta_setpoint < 0:

                            if delta_temperature is not None and delta_temperature < 0:

                                duration_of_manual_mode = abs(delta_temperature / cooling_rate) + 1.0 if cooling_rate < 0 else float(1.0)

                            else:
                                duration_of_manual_mode = 1.0  # Default to 1 hour if current temp is below setpoint
                            
                            if duration_of_manual_mode > 3.0:
                                duration_of_manual_mode = 3.0  # Cap at 3 hours

                        else:
                            duration_of_manual_mode = 1.0  # Default to 1 hour if unknown

                        print(f'Duration of manual mode based on current temp and setpoint: {duration_of_manual_mode:.1f} hours')

                        query_manual_start = f'''
                                    SELECT LAST("system_mode") as "system_mode"
                                    FROM {INFLUXDB_GD_DATABASE}."autogen"."system_info" 
                                    WHERE "{room}"= \'{radiator}\' 
                                    AND "system_mode" != 'MANUAL'
                                    AND time > now() - 24h
                                '''
                        query_manual_start = client_influx.query(query_manual_start)
                        manual_start_data = list(query_manual_start.get_points())
                        print('LAST NON MANUAL MODE', manual_start_data)

                        if not manual_start_data:
                            query_manual_start = f'''
                                    SELECT FIRST("system_mode") as "system_mode"
                                    FROM {INFLUXDB_GD_DATABASE}."autogen"."system_info" 
                                    WHERE "{room}"= \'{radiator}\' 
                                    AND "system_mode" = 'MANUAL'
                                    AND time > now() - 24h
                                '''
                            query_manual_start = client_influx.query(query_manual_start)
                            manual_start_data = list(query_manual_start.get_points())
                            print('FIRST MANUAL MODE', manual_start_data)

                        if manual_start_data and manual_start_data[0]['time']:
                            manual_start_time = datetime.fromisoformat(manual_start_data[0]['time'].replace("Z", "+00:00"))
                            my_timezone = pytz.timezone('Europe/Berlin')
                            manual_start_time = manual_start_time.astimezone(my_timezone)
                            current_time = datetime.now(local_tz)
                            time_in_manual = (current_time - manual_start_time) + timedelta(minutes=1)
                            print(f'Manual mode started at: {manual_start_time}, duration: {time_in_manual}')

                            should_switch_to_ai = False

                            if time_in_manual > timedelta(hours=duration_of_manual_mode):
                                should_switch_to_ai = True
                                system_mode = "AI"
                                radiators_mode.append("AI")
                                sub_system_mode = system_info[0]['sub_system_mode'] if system_info else "idle"
                                mode_change_to = "AI"
                                mode_change.append(True)
                                print(f"It has been more than {duration_of_manual_mode:.1f} hours since manual mode started - switching to AI")

                            else:
                                # Calculate when manual mode will end
                                manual_end_time = manual_start_time + timedelta(hours=duration_of_manual_mode)
                                
                                # Format the end time as HH:MM
                                end_manual = manual_end_time.strftime('%H:%M')
                                
                                print(f"Manual mode will end at: {end_manual}")

                                print('end_of_manual', end_manual)
                                radiators_mode.append("MANUAL")
                                system_mode = "MANUAL"
                                sub_system_mode = "manual"
                                mode_change.append(False)
                                print(f"It has been less than {duration_of_manual_mode:.1f} hours since manual mode started - keeping manual mode")

                        # print(f'Temperature threshold: {temperature_threshold}°C')

                        # setpoint_matches = (db_setpoint is not None and 
                        #                 abs(current_setpoint_float - db_setpoint) < 0.1)
                        
                        # should_switch_to_ai = False
                        
                        # if db_system_mode == 'MANUAL' and current_temp is not None:
                        #     print(f'Current temperature: {current_temp}°C')

                        #     if current_temp >= temperature_threshold:
                        #         print('Temperature is above threshold, checking duration since manual mode started...')
                                
                        #         # STEP 1: Find when manual mode started (last non-MANUAL before current manual)
                        #         query_manual_start = f'''
                        #             SELECT LAST("system_mode") as "system_mode"
                        #             FROM {INFLUXDB_GD_DATABASE}."autogen"."system_info" 
                        #             WHERE "{room}"= \'{radiator}\' 
                        #             AND "system_mode" != 'MANUAL'
                        #             AND time > now() - 24h
                        #         '''
                        #         query_manual_start = client_influx.query(query_manual_start)
                        #         manual_start_data = list(query_manual_start.get_points())
                        #         print('manual_start_data', manual_start_data)
                                
                        #         if manual_start_data and manual_start_data[0]['system_mode']:
                        #             manual_start_time = datetime.fromisoformat(manual_start_data[0]['time'].replace("Z", "+00:00"))
                        #             my_timezone = pytz.timezone('Europe/Berlin')
                        #             manual_start_time = manual_start_time.astimezone(my_timezone)
                        #             current_time = datetime.now(local_tz)
                        #             print(f'Manual mode started at: {manual_start_time}')

                        #             sensor_table = sensors_table(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)

                        #             temp_sensor_id = [sensor['sensor_id'] for sensor in sensor_table if sensor['room_id'] == room and (sensor['sensor_type'] == "Smart air house keeper" or sensor['sensor_type'] == "Temperature and humidity sensor with screen" or sensor['sensor_type'] == "Temperature and humidity sensor") ]
                        #             print(temp_sensor_id)
                        #             if temp_sensor_id:

                        #                 client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST,
                        #                                     port=INFLUXDB_PORT,
                        #                                     username=INFLUXDB_USERNAME,
                        #                                     password=INFLUXDB_PASSWORD,
                        #                                     database=INFLUXDB_DATABASE)
                                        
                        #                 entity_id = temp_sensor_id[0] + '_temperature'
                                    
                        #                 # STEP 2: Find when temperature FIRST reached ≥ threshold SINCE manual mode started
                        #                 query_temp_duration = f'''
                        #                     SELECT FIRST("value") as "first_temp_above"
                        #                     FROM {INFLUXDB_DATABASE}."autogen"."°C" 
                        #                     WHERE time >= \'{manual_start_data[0]['time']}\'
                        #                     AND "entity_id"=\'{entity_id}\' 
                        #                     AND value >= {temperature_threshold}
                        #                 '''

                        #                 print('query_temp_duration', query_temp_duration)
                        #                 query_temp_duration = client_influx.query(query_temp_duration)
                        #                 temp_duration_data = list(query_temp_duration.get_points())
                        #                 print('temp_duration_data since manual start', temp_duration_data)

                        #             if not temp_duration_data:

                        #                 time_since_manual = current_time - manual_start_time
                        #                 print("current_time", current_time, "manual_start_time", manual_start_time)

                        #                 # Check if it's been more than 1 hour
                        #                 if time_since_manual > timedelta(minutes=60):
                        #                     should_switch_to_ai = True
                        #                     system_mode = "AI"
                        #                     radiators_mode.append("AI")
                        #                     sub_system_mode = system_info[0]['sub_system_mode'] if system_info else "idle"
                        #                     mode_change_to = "AI"
                        #                     mode_change.append(True)
                        #                     print(f"Temperature ≥{temperature_threshold}°C for more than 1 hour since manual mode started - switching to AI")

                        #                 else:
                        #                     radiators_mode.append("MANUAL")
                        #                     system_mode = "MANUAL"
                        #                     sub_system_mode = "manual"
                        #                     mode_change.append(False)
                        #                     print(f"Temperature was already above {temperature_threshold}°C before manual but for less than 1 hour since manual mode started - keeping manual mode")
                        #                 print(f"No temperature ≥{temperature_threshold}°C found since manual mode started")
                                    
                        #             if temp_duration_data and temp_duration_data[0]['time']:
                        #                 first_above_time = datetime.fromisoformat(temp_duration_data[0]['time'].replace("Z", "+00:00"))
                        #                 first_above_time = first_above_time.astimezone(my_timezone)
                                        
                        #                 # Calculate how long temperature has been ≥ threshold SINCE manual mode
                        #                 time_above_threshold = current_time - first_above_time
                        #                 print('first_above_time', first_above_time)
                        #                 print(f'Temperature has been ≥{temperature_threshold}°C for: {time_above_threshold} (since manual mode)')
                                        
                        #                 # Check if it's been more than 1 hour
                        #                 if time_above_threshold > timedelta(minutes=60):
                        #                     should_switch_to_ai = True
                        #                     system_mode = "AI"
                        #                     radiators_mode.append("AI")
                        #                     sub_system_mode = system_info[0]['sub_system_mode'] if system_info else "idle"
                        #                     mode_change_to = "AI"
                        #                     mode_change.append(True)
                        #                     print(f"Temperature ≥{temperature_threshold}°C for more than 1 hour since manual mode started - switching to AI")

                        #                 else:
                        #                     radiators_mode.append("MANUAL")
                        #                     system_mode = "MANUAL"
                        #                     sub_system_mode = "manual"
                        #                     mode_change.append(False)
                        #                     print(f"Temperature ≥{temperature_threshold}°C for less than 1 hour since manual mode started - keeping manual mode")


                                #     else:
                                #         print(f"No temperature ≥{temperature_threshold}°C found since manual mode started")
                                # else:
                                #     print("Could not determine when manual mode started")
                                    # sensor_table = sensors_table(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)

                                    # temp_sensor_id = [sensor['sensor_id'] for sensor in sensor_table if sensor['room_id'] == room and (sensor['sensor_type'] == "Smart air house keeper" or sensor['sensor_type'] == "Temperature and humidity sensor with screen" or sensor['sensor_type'] == "Temperature and humidity sensor") ]
                                    # print(temp_sensor_id)
                                    # if temp_sensor_id:

                                    #     client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST,
                                    #                         port=INFLUXDB_PORT,
                                    #                         username=INFLUXDB_USERNAME,
                                    #                         password=INFLUXDB_PASSWORD,
                                    #                         database=INFLUXDB_DATABASE)
                                        
                                    #     entity_id = temp_sensor_id[0] + '_temperature'
                                    
                                    # # Check how long temperature has been ≥ threshold
                                    # query_temp_duration = f'''
                                    #     SELECT FIRST("value") as "first_temp_above"
                                    #     FROM "ga_homeassistant_db"."autogen"."°C"
                                    #     WHERE "entity_id"=\'{entity_id}\' 
                                    #     AND value >= {temperature_threshold}
                                    # '''
                                    # #AND time >= \'{manual_start_time}\'
                                    # print('query_temp_duration', query_temp_duration)
                                    # query_temp_duration = client_influx.query(query_temp_duration)
                                    # temp_duration_data = list(query_temp_duration.get_points())
                                    
                                    # if temp_duration_data and temp_duration_data[0]['time']:
                                    #     first_above_time = datetime.fromisoformat(temp_duration_data[0]['time'].replace("Z", "+00:00"))
                                    #     first_above_time = first_above_time.astimezone(my_timezone)
                                        
                                    #     # Calculate how long temperature has been ≥ threshold
                                    #     time_above_threshold = current_time - first_above_time
                                    #     print(f'Temperature has been ≥{temperature_threshold}°C for: {time_above_threshold}')
                                        
                                    #     # Check if it's been more than 1 hour
                                    #     if time_above_threshold > timedelta(hours=1):
                                    #         should_switch_to_ai = True
                                    #         print(f"Temperature ≥{temperature_threshold}°C for more than 1 hour ({int(time_above_threshold.total_seconds()/3600)} hours) - will switch to AI")

                                    #         system_mode = "AI"
                                    #         radiators_mode.append("AI")
                                    #         sub_system_mode = system_info[0]['sub_system_mode'] if system_info else "idle"
                                    #         mode_change_to = "AI"
                                    #         mode_change.append(True)

                            #         else:
                            #             radiators_mode.append("MANUAL")
                            #             system_mode = "MANUAL"
                            #             sub_system_mode = "manual"
                            #             mode_change.append(False)
                            #             print(f"Temperature ≥{temperature_threshold}°C for less than 1 hour on Manual mode - keeping manual mode")

                            #     else:
                            #         radiators_mode.append("MANUAL")
                            #         system_mode = "MANUAL"
                            #         sub_system_mode = "manual"
                            #         mode_change.append(False)
                            #         print("Could not determine manual session start time")
                            # else:
                            #     radiators_mode.append("MANUAL")
                            #     system_mode = "MANUAL"
                            #     sub_system_mode = "manual"
                            #     mode_change.append(False)
                            #     print(f"Current temperature ({current_temp}°C) is below threshold ({temperature_threshold}°C)")
                        
                        # Decision logic remains the same...
                        else:
                            # New manual override or inconsistent state
                            radiators_mode.append("MANUAL")
                            system_mode = "MANUAL"
                            sub_system_mode = "manual"
                            mode_change.append(True)
                            mode_change_to = "MANUAL"
                            print(f"Mode: MANUAL (new override or change detected)")
                        
                    else:
                        # Setpoint does not match, treat as new manual override
                        radiators_mode.append("MANUAL")
                        system_mode = "MANUAL"
                        sub_system_mode = "manual"
                        mode_change.append(True)
                
                elif thermostat_mode == 'auto':
                    # Thermostat is in AUTO mode
                    if db_system_mode == 'AI':
                        # Consistent AI mode
                        radiators_mode.append("AI")
                        system_mode = "AI"
                        sub_system_mode = system_info[0]['sub_system_mode'] if system_info else "idle"
                        mode_change.append(False)
                        print(f"Mode: AI (consistent with database)")
                    elif db_system_mode == 'MANUAL':
                        # Was manual, now switched to auto
                        radiators_mode.append("AI")
                        system_mode = "AI"
                        sub_system_mode = "idle"
                        mode_change.append(True)
                        mode_change_to = "AI"
                        print(f"Mode: AI (switched from MANUAL)")
                    else:
                        # Default to AI
                        radiators_mode.append("AI")
                        system_mode = "AI"
                        sub_system_mode = "idle"
                        mode_change.append(True)
                        mode_change_to = "AI"
                        print(f"Mode: AI (default)")
                
                next_time = system_info[0]['next_time']

                next_time = get_hour_minute(next_time)

                duration_of_last_manual_str = '0 Minuten'
                
                # # Get last manual mode time and duration
                # if system_mode != "MANUAL":
                #     query_last_manual_mode = f'SELECT LAST("system_mode") AS "system_mode" FROM {INFLUXDB_GD_DATABASE}."autogen"."system_info" WHERE "{room}"= \'{radiator}\' and "system_mode" = \'MANUAL\' '
                #     query_last_manual_mode = client_influx.query(query_last_manual_mode)
                #     last_manual_mode = list(query_last_manual_mode.get_points())
                #     print('last manual mode', last_manual_mode)

                #     if not last_manual_mode:
                #         last_manual_mode_time_str = 'mehr als 1 Tag'
                #         duration_of_last_manual_str = '0 Minuten'
                #     else:
                #         last_manual_mode_time = last_manual_mode[0]['time']
                #         # Convert the string to a datetime object (assuming UTC)
                #         last_manual_mode_time = datetime.fromisoformat(last_manual_mode_time.replace("Z", "+00:00"))
                #         # Convert the timestamp to the same timezone as current_time
                #         my_timezone = pytz.timezone('Europe/Berlin')
                #         last_manual_mode_time = last_manual_mode_time.astimezone(my_timezone)
                #         current_time = datetime.now(timezone.utc)
                #         # Calculate the time difference
                #         last_manual_mode_time_difference = current_time - last_manual_mode_time
                #         print('time_difference', last_manual_mode_time_difference)
                        
                #         if last_manual_mode_time_difference > timedelta(0):
                #             if last_manual_mode_time_difference < timedelta(minutes=60):
                #                 # Extract hours and minutes from the difference
                #                 hours, remainder = divmod(last_manual_mode_time_difference.total_seconds(), 3600)
                #                 minutes, _ = divmod(remainder, 60)
                #                 print(f"The given datetime is {int(hours)} hours and {int(minutes)} minutes from now.")
                #                 last_manual_mode_time_str = f'{int(minutes)} Minuten'
                #             elif last_manual_mode_time_difference < timedelta(minutes=1440):
                #                 # Extract hours and minutes from the difference
                #                 hours, remainder = divmod(last_manual_mode_time_difference.total_seconds(), 3600)
                #                 minutes, _ = divmod(remainder, 60)
                #                 print(f"The given datetime is {int(hours)} hours and {int(minutes)} minutes from now.")
                #                 last_manual_mode_time_str = f'{int(hours)} Stunde'
                #             elif last_manual_mode_time_difference > timedelta(minutes=1440):
                #                 last_manual_mode_time_str = 'mehr als 1 Tag'
                #             else: 
                #                 last_manual_mode_time_str = 'weniger als 5 Minuten'
                #         else: 
                #             last_manual_mode_time_str = 'weniger als 5 Minuten'

                #         if 'time' in last_manual_mode[0]:
                #             last_manual_timestamp_str = last_manual_mode[0]['time']
                #             formatted_time = datetime.strptime(last_manual_timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ").isoformat() + "Z"

                #             query_last_non_manual_mode = f'''SELECT LAST("system_mode") AS "system_mode" FROM {INFLUXDB_GD_DATABASE}."autogen"."system_info" WHERE time < '{formatted_time}' AND "{room}"= \'room\' and "system_mode" != \'MANUAL\' '''
                #             query_last_non_manual_mode = client_influx.query(query_last_non_manual_mode)
                #             last_non_manual_mode = list(query_last_non_manual_mode.get_points())

                #             print('last_non_manual_mode', last_non_manual_mode)

                #             if last_non_manual_mode and 'time' in last_non_manual_mode[0]:
                #                 last_non_manual_timestamp_str = last_non_manual_mode[0]['time']
                #                 format = "%Y-%m-%dT%H:%M:%S.%fZ"
                #                 last_manual_timestamp = datetime.strptime(last_manual_timestamp_str, format)
                #                 last_non_manual_timestamp = datetime.strptime(last_non_manual_timestamp_str, format)

                #                 print(last_manual_timestamp, last_non_manual_timestamp)

                #                 duration_of_last_manual = last_manual_timestamp - last_non_manual_timestamp

                #                 print('duration_of_last_manual', room, duration_of_last_manual)

                #                 if duration_of_last_manual > timedelta(0):
                #                     total_seconds = duration_of_last_manual.total_seconds()
                #                     total_minutes = total_seconds / 60
                                    
                #                     if duration_of_last_manual < timedelta(minutes=60):
                #                         minutes = int(total_minutes)
                #                         print(f"Dauer: {minutes} Minuten")
                #                         duration_of_last_manual_str = f'{minutes} Minuten'
                                    
                #                     elif duration_of_last_manual < timedelta(minutes=1440):
                #                         hours = int(total_minutes // 60)
                #                         minutes = int(15 * round((total_minutes % 60) / 15))
                                        
                #                         if minutes == 60:
                #                             hours += 1
                #                             minutes = 0
                                        
                #                         if minutes == 0:
                #                             duration_str = f'{hours} Stunde{"n" if hours > 1 else ""}'
                #                         else:
                #                             duration_str = f'{hours} Stunde{"n" if hours > 1 else ""} und {minutes} Minuten'
                                        
                #                         print(f"Dauer: {duration_str}")
                #                         duration_of_last_manual_str = duration_str
                                    
                #                     else:
                #                         days = int(total_minutes // 1440)
                #                         remaining_hours = int((total_minutes % 1440) // 60)
                                        
                #                         if remaining_hours == 0:
                #                             duration_str = f'{days} Tag{"e" if days > 1 else ""}'
                #                         else:
                #                             duration_str = f'{days} Tag{"e" if days > 1 else ""} und {remaining_hours} Stunde{"n" if remaining_hours > 1 else ""}'
                                        
                #                         print(f"Dauer: {duration_str}")
                #                         duration_of_last_manual_str = duration_str
                #                 else:
                #                     duration_of_last_manual_str = '0 Minuten'
                #             else:
                #                 duration_of_last_manual_str = '0 Minuten'
                
                # Write to database immediately to maintain consistent state
                client = connect_or_create_database(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)
                print("system_mode", system_mode)
                print("sub_system_mode", sub_system_mode)
                
                json_body = [{
                    "measurement": 'system_info',
                    "tags": {f'{room}' : f"{radiator}"},
                    "fields": {'system_mode': system_mode, 'sub_system_mode': sub_system_mode, 'system_setpoint' : float(current_target), 'next_time' : f"{next_time}"},
                    "time": datetime.now(timezone.utc).isoformat()
                }]
                    
                client.write_points(json_body)
                client.close()

                # Write data to INFLUXDB_DATABASE too
                client = connect_or_create_database(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_DATABASE)
                
                json_body = [{
                    "measurement": 'system_info',
                    "tags": {f'{room}' : f"{radiator}"},
                    "fields": {'system_mode': system_mode, 'sub_system_mode': sub_system_mode, 'system_setpoint' : float(current_target), 'next_time' : f"{next_time}"},
                    "time": datetime.now(timezone.utc).isoformat()
                }]
                    
                client.write_points(json_body)
                client.close()
               
    print('IS HEATING', is_heating)
    
    if radiators_mode and current_target:
        mode_change = any(mode_change)
        print('changed mode to', mode_change_to)

        print('is_heating_list', is_heating)
        
        is_heating = all(is_heating)
        print('is_heating', is_heating)

        if mode_change and mode_change_to is not None:
            return mode_change_to, float(current_target), mode_change, end_manual, last_manual_mode_time_str, duration_of_last_manual_str, max_valve_opening, is_heating

        if "MANUAL" in radiators_mode:
            return "MANUAL", float(current_target), mode_change, end_manual, last_manual_mode_time_str, duration_of_last_manual_str, max_valve_opening, is_heating
    
        else:
            return "AI", float(current_target), mode_change, end_manual, last_manual_mode_time_str, duration_of_last_manual_str, max_valve_opening, is_heating

    # Return default values if nothing found
    return None, None, False, end_manual, last_manual_mode_time_str, duration_of_last_manual_str, max_valve_opening, is_heating
def room_mode_intermediate(room, room_radiators):
    # Initialize all return variables at the start
    radiators_mode = []
    mode_change = []
    end_manual = 'unknown'  # Default value
    mode_change_to = None
    last_manual_mode_time_str = 'mehr als 1 Tag'  # Default value
    duration_of_last_manual_str = '0 Minuten'  # Default value
    max_valve_opening = 100.0  # Default value
    current_target = None

    for radiator in room_radiators:
        client_influx = connect_or_create_tag(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE, "system_info", room, radiator)

        query_system_info = f'SELECT LAST("system_setpoint") AS "system_setpoint", ("system_mode") AS "system_mode", ("next_time") AS "next_time", ("sub_system_mode") AS "sub_system_mode" FROM {INFLUXDB_GD_DATABASE}."autogen"."system_info" WHERE "{room}"= \'{radiator}\' '
        query_system_info = client_influx.query(query_system_info)
        system_info = list(query_system_info.get_points())
        print('system_info', system_info)

        client_influx.close()
        
        data = radiator_status(radiator)

        if data:
            current_setpoint = data['occupied_heating_setpoint']
            current_target = current_setpoint  # Store for return
            
            # Calculate the duration of the current session of manual mode
            if data['system_mode'] == 'heat':
                query_system_mode = f'SELECT LAST("system_mode") AS "system_mode" FROM {INFLUXDB_GD_DATABASE}."autogen"."system_info" WHERE "{room}"=\'room\' and "system_mode" = \'AI\' '
                query_system_mode = client_influx.query(query_system_mode)
                system_mode_result = list(query_system_mode.get_points())
                print('system mode 1', system_mode_result)

                if not system_mode_result:

                    query_system_mode = f'SELECT FIRST("system_mode") AS "system_mode" FROM {INFLUXDB_GD_DATABASE}."autogen"."system_info" WHERE "{room}"=\'room\' and "system_mode" = \'MANUAL\' '
                    query_system_mode = client_influx.query(query_system_mode)
                    system_mode_result = list(query_system_mode.get_points())
                    print('system mode 2', system_mode_result)

                if system_mode_result:

                    manual_start_time = datetime.fromisoformat(system_mode_result[0]['time'].replace("Z", "+00:00"))
                    my_timezone = pytz.timezone('Europe/Berlin')
                    manual_start_time = manual_start_time.astimezone(my_timezone)

                    timestamp_str = system_mode_result[0]['time']
                    # Convert the string to a datetime object (assuming UTC)
                    timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                    # Convert the timestamp to the same timezone as current_time
                    my_timezone = pytz.timezone('Europe/Berlin')
                    timestamp = timestamp.astimezone(my_timezone)
                    current_time = datetime.now(timezone.utc)
                    # Calculate the time difference
                    time_difference = current_time - timestamp
                    print('time_difference', time_difference)

                    time_difference_end_of_manual = timedelta(minutes=180) - time_difference

                    if time_difference_end_of_manual > timedelta(0):
                        # Extract hours and minutes from the difference
                        hours, remainder = divmod(time_difference_end_of_manual.total_seconds(), 3600)
                        minutes, _ = divmod(remainder, 60)
                        print(f"The given datetime is {int(hours)} hours and {int(minutes)} minutes from now.")
                        end_manual = f'{int(hours)} Stunde und {int(minutes)} Minuten'
                    else: 
                        end_manual = 'weniger als 5 Minuten'
                    
                    print('end_of_manual', end_manual)

                    # Check if the difference is more than 1 hour
                    if time_difference < timedelta(minutes=180):
                        radiators_mode.append("MANUAL")
                        system_mode = "MANUAL"
                        sub_system_mode = "manual"
                        mode_change.append(False)
                    else:
                        radiators_mode.append("AI")
                        system_mode = "AI"
                        sub_system_mode = "idle"
                        mode_change.append(True)
                        mode_change_to = "AI"
                else:
                    # Handle case when no system_mode_result
                    radiators_mode.append("MANUAL")
                    system_mode = "MANUAL"
                    sub_system_mode = "manual"
                    mode_change.append(False)

            elif data['system_mode'] == 'auto':
                radiators_mode.append("AI")
                system_mode = "AI"
                sub_system_mode = "idle"
                mode_change.append(False)

            else: continue
                
            # # Get last manual mode time and duration
            # if system_info[0]['system_mode'] != "MANUAL":
            #     query_last_manual_mode = f'SELECT LAST("system_mode") AS "system_mode" FROM {INFLUXDB_GD_DATABASE}."autogen"."system_info" WHERE "{room}"= \'{radiator}\' and "system_mode" = \'MANUAL\' '
            #     query_last_manual_mode = client_influx.query(query_last_manual_mode)
            #     last_manual_mode = list(query_last_manual_mode.get_points())
            #     print('last manual mode', last_manual_mode)

            #     if not last_manual_mode:
            #         last_manual_mode_time_str = 'mehr als 1 Tag'
            #         duration_of_last_manual_str = '0 Minuten'
            #     else:
            #         last_manual_mode_time = last_manual_mode[0]['time']
            #         # Convert the string to a datetime object (assuming UTC)
            #         last_manual_mode_time = datetime.fromisoformat(last_manual_mode_time.replace("Z", "+00:00"))
            #         # Convert the timestamp to the same timezone as current_time
            #         my_timezone = pytz.timezone('Europe/Berlin')
            #         last_manual_mode_time = last_manual_mode_time.astimezone(my_timezone)
            #         current_time = datetime.now(timezone.utc)
            #         # Calculate the time difference
            #         last_manual_mode_time_difference = current_time - last_manual_mode_time
            #         print('time_difference', last_manual_mode_time_difference)
                    
            #         if last_manual_mode_time_difference > timedelta(0):
            #             if last_manual_mode_time_difference < timedelta(minutes=60):
            #                 # Extract hours and minutes from the difference
            #                 hours, remainder = divmod(last_manual_mode_time_difference.total_seconds(), 3600)
            #                 minutes, _ = divmod(remainder, 60)
            #                 print(f"The given datetime is {int(hours)} hours and {int(minutes)} minutes from now.")
            #                 last_manual_mode_time_str = f'{int(minutes)} Minuten'
            #             elif last_manual_mode_time_difference < timedelta(minutes=1440):
            #                 # Extract hours and minutes from the difference
            #                 hours, remainder = divmod(last_manual_mode_time_difference.total_seconds(), 3600)
            #                 minutes, _ = divmod(remainder, 60)
            #                 print(f"The given datetime is {int(hours)} hours and {int(minutes)} minutes from now.")
            #                 last_manual_mode_time_str = f'{int(hours)} Stunde'
            #             elif last_manual_mode_time_difference > timedelta(minutes=1440):
            #                 last_manual_mode_time_str = 'mehr als 1 Tag'
            #             else: 
            #                 last_manual_mode_time_str = 'weniger als 5 Minuten'
            #         else: 
            #             last_manual_mode_time_str = 'weniger als 5 Minuten'

            #         if 'time' in last_manual_mode[0]:
            #             last_manual_timestamp_str = last_manual_mode[0]['time']
            #             formatted_time = datetime.strptime(last_manual_timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ").isoformat() + "Z"

            #             query_last_non_manual_mode = f'''SELECT LAST("system_mode") AS "system_mode" FROM {INFLUXDB_GD_DATABASE}."autogen"."system_info" WHERE time < '{formatted_time}' AND "{room}"= \'room\' and "system_mode" != \'MANUAL\' '''
            #             query_last_non_manual_mode = client_influx.query(query_last_non_manual_mode)
            #             last_non_manual_mode = list(query_last_non_manual_mode.get_points())

            #             print('last_non_manual_mode', last_non_manual_mode)

            #             if last_non_manual_mode and 'time' in last_non_manual_mode[0]:
            #                 last_non_manual_timestamp_str = last_non_manual_mode[0]['time']
            #                 format = "%Y-%m-%dT%H:%M:%S.%fZ"
            #                 last_manual_timestamp = datetime.strptime(last_manual_timestamp_str, format)
            #                 last_non_manual_timestamp = datetime.strptime(last_non_manual_timestamp_str, format)

            #                 print(last_manual_timestamp, last_non_manual_timestamp)

            #                 duration_of_last_manual = last_manual_timestamp - last_non_manual_timestamp

            #                 print('duration_of_last_manual', room, duration_of_last_manual)

            #                 if duration_of_last_manual > timedelta(0):
            #                     total_seconds = duration_of_last_manual.total_seconds()
            #                     total_minutes = total_seconds / 60
                                
            #                     if duration_of_last_manual < timedelta(minutes=60):
            #                         minutes = int(total_minutes)
            #                         print(f"Dauer: {minutes} Minuten")
            #                         duration_of_last_manual_str = f'{minutes} Minuten'
                                
            #                     elif duration_of_last_manual < timedelta(minutes=1440):
            #                         hours = int(total_minutes // 60)
            #                         minutes = int(15 * round((total_minutes % 60) / 15))
                                    
            #                         if minutes == 60:
            #                             hours += 1
            #                             minutes = 0
                                    
            #                         if minutes == 0:
            #                             duration_str = f'{hours} Stunde{"n" if hours > 1 else ""}'
            #                         else:
            #                             duration_str = f'{hours} Stunde{"n" if hours > 1 else ""} und {minutes} Minuten'
                                    
            #                         print(f"Dauer: {duration_str}")
            #                         duration_of_last_manual_str = duration_str
                                
            #                     else:
            #                         days = int(total_minutes // 1440)
            #                         remaining_hours = int((total_minutes % 1440) // 60)
                                    
            #                         if remaining_hours == 0:
            #                             duration_str = f'{days} Tag{"e" if days > 1 else ""}'
            #                         else:
            #                             duration_str = f'{days} Tag{"e" if days > 1 else ""} und {remaining_hours} Stunde{"n" if remaining_hours > 1 else ""}'
                                    
            #                         print(f"Dauer: {duration_str}")
            #                         duration_of_last_manual_str = duration_str
            #                 else:
            #                     duration_of_last_manual_str = '0 Minuten'
            #             else:
            #                 duration_of_last_manual_str = '0 Minuten'
            
            next_time = system_info[0]['next_time']

            next_time = get_hour_minute(next_time)
            
            # Write to database
            client = connect_or_create_database(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)
            print("system_mode", system_mode)
            print("sub_system_mode", sub_system_mode)
            
            json_body = [{
                "measurement": 'system_info',
                "tags": {f'{room}' : f"{radiator}"},
                "fields": {'system_mode': system_mode, 'sub_system_mode': sub_system_mode, 'system_setpoint' : float(current_target), 'next_time' : f"{next_time}"},
                "time": datetime.now(timezone.utc).isoformat()
            }]
                
            client.write_points(json_body)
            client.close()

            # Write data to INFLUXDB_DATABASE too
            client = connect_or_create_database(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_DATABASE)
            
            json_body = [{
                "measurement": 'system_info',
                "tags": {f'{room}' : f"{radiator}"},
                "fields": {'system_mode': system_mode, 'sub_system_mode': sub_system_mode, 'system_setpoint' : float(current_target), 'next_time' : f"{next_time}"},
                "time": datetime.now(timezone.utc).isoformat()
            }]
                
            client.write_points(json_body)
            client.close()
           
            if system_mode == 'MANUAL':

                mode = 'heat'

                MQTT_TOPIC = 'zigbee2mqtt/' + radiator + '/set'

                PAYLOAD = {"system_mode" : mode
                            }      

                # Create MQTT client instance
                mqtt_client = mqtt.Client()

                # Set username and password if needed
                mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

                # Set the connection and message callbacks
                mqtt_client.on_connect = lambda mqtt_client, userdata, flags, rc: on_connect_radiator(mqtt_client, userdata, flags, rc, MQTT_TOPIC, PAYLOAD)
                mqtt_client.on_message = lambda mqtt_client, userdata, msg: on_message_radiator(mqtt_client, userdata, msg)

                # Connect to the MQTT broker
                mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)

                # Start the MQTT loop to handle incoming messages
                mqtt_client.loop_start()

                time.sleep(2)  # Avoid tight loop

                mqtt_client.loop_stop()
                mqtt_client.disconnect()
                
            elif system_mode == 'AI':
            
                mode = 'auto'

                MQTT_TOPIC = 'zigbee2mqtt/' + radiator + '/set'

                PAYLOAD = {"system_mode" : mode}      

                # Create MQTT client instance
                mqtt_client = mqtt.Client()

                # Set username and password if needed
                mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

                # Set the connection and message callbacks
                mqtt_client.on_connect = lambda mqtt_client, userdata, flags, rc: on_connect_radiator(mqtt_client, userdata, flags, rc, MQTT_TOPIC, PAYLOAD)
                mqtt_client.on_message = lambda mqtt_client, userdata, msg: on_message_radiator(mqtt_client, userdata, msg)

                # Connect to the MQTT broker
                mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)

                # Start the MQTT loop to handle incoming messages
                mqtt_client.loop_start()

                time.sleep(2)  # Avoid tight loop

                mqtt_client.loop_stop()
                mqtt_client.disconnect()

                client = connect_or_create_database(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)
            
                json_body = [{
                    "measurement": 'system_info',
                    "tags": {f'{room}' : "room"},
                    "fields": {'system_mode': system_mode, 'system_setpoint' : float(current_target), 'next_time' : f"{next_time}", 'end_of_manual': end_manual, 'last_manual': last_manual_mode_time_str, 'duration_of_last_manual': duration_of_last_manual_str},
                    "time": datetime.now(timezone.utc).isoformat()
                }]
                    
                client.write_points(json_body)
                client.close()

                client = connect_or_create_database(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_DATABASE)
                
                json_body = [{
                    "measurement": 'system_info',
                    "tags": {f'{room}' : "room"},
                    "fields": {'system_mode': system_mode, 'system_setpoint' : float(current_target), 'next_time' : f"{next_time}", 'end_of_manual': end_manual, 'last_manual': last_manual_mode_time_str, 'duration_of_last_manual': duration_of_last_manual_str},
                    "time": datetime.now(timezone.utc).isoformat()
                }]
                    
                client.write_points(json_body)
                client.close()


def control_radiator(room, current_temp, room_radiators, parsed_schedule, current_time, heating_rate, cooling_rate, secondary_thu_offset):

    actual_room_mode, current_target, mode_change, end_manual, last_manual_mode_time_str, duration_of_last_manual_str, max_valve_opening, is_heating = room_mode(room, room_radiators, current_temp, heating_rate, cooling_rate)

    if actual_room_mode is not None:

        if actual_room_mode == 'MANUAL':

            new_sub_system_mode, valve_opening = heating_decision_manual(current_temp, is_heating, current_target, max_valve_opening)

            next_time = None

            temp_difference = None

            new_target = current_target
        
        else:

            

            new_sub_system_mode, valve_opening, next_time, temp_difference, new_target = heating_decision_ai(room, parsed_schedule, current_temp, is_heating, current_time, heating_rate, cooling_rate, max_valve_opening)
            
            next_time = get_hour_minute(next_time)

        if None in (new_sub_system_mode, valve_opening):

            status = False
        
        else:
        
            status, occupancy_offset = local_temperature_calibration(room, new_sub_system_mode, actual_room_mode, valve_opening, temp_difference, new_target, mode_change, secondary_thu_offset)
        
        if status:
            client = connect_or_create_database(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)
            
            json_body = [{
                "measurement": 'system_info',
                "tags": {f'{room}' : "room"},
                "fields": {'system_mode': actual_room_mode, 'sub_system_mode': new_sub_system_mode, 'system_setpoint' : float(current_target), 'next_time' : f"{next_time}", 'new_target' : f"{new_target}", 'end_of_manual': end_manual, 'last_manual': last_manual_mode_time_str, 'duration_of_last_manual': duration_of_last_manual_str, 'occupancy_offset' : f"{occupancy_offset}"},
                "time": datetime.now(timezone.utc).isoformat()
            }]
                
            client.write_points(json_body)
            client.close()

            client = connect_or_create_database(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_DATABASE)
            
            json_body = [{
                "measurement": 'system_info',
                "tags": {f'{room}' : "room"},
                "fields": {'system_mode': actual_room_mode, 'sub_system_mode': new_sub_system_mode, 'system_setpoint' : float(current_target), 'next_time' : f"{next_time}", 'new_target' : f"{new_target}", 'end_of_manual': end_manual, 'last_manual': last_manual_mode_time_str, 'duration_of_last_manual': duration_of_last_manual_str, 'occupancy_offset' : f"{occupancy_offset}"},
                "time": datetime.now(timezone.utc).isoformat()
            }]
                
            client.write_points(json_body)
            client.close()

def temperature_sensor_offline(radiators_id_list):

    for radiator in radiators_id_list:

        MQTT_TOPIC = 'zigbee2mqtt/' + radiator + '/set'

        PAYLOAD = {"local_temperature_calibration": 0
                    }      

        # Create MQTT client instance
        mqtt_client = mqtt.Client()

        # Set username and password if needed
        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

        # Set the connection and message callbacks
        mqtt_client.on_connect = lambda mqtt_client, userdata, flags, rc: on_connect_radiator(mqtt_client, userdata, flags, rc, MQTT_TOPIC, PAYLOAD)
        mqtt_client.on_message = lambda mqtt_client, userdata, msg: on_message_radiator(mqtt_client, userdata, msg)

        # Connect to the MQTT broker
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)

        # Start the MQTT loop to handle incoming messages
        mqtt_client.loop_start()

        time.sleep(2)  # Avoid tight loop

        mqtt_client.loop_stop()
        mqtt_client.disconnect()

def heating_management():
    
    sensor_table = sensors_table(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)

    rooms_id =  set(sensor['room_id'] for sensor in sensor_table if sensor['room_id'] != 'OUT' )
    print(rooms_id)
        
    for room in rooms_id:

        if room == 'livingroom':

            print('room', room)

            radiators_id_list = [sensor['sensor_id'] for sensor in sensor_table if sensor['room_id'] == room and sensor['sensor_type'] == "Zigbee thermostatic radiator valve"]
            print(radiators_id_list)

            temp_sensor_id = [sensor['sensor_id'] for sensor in sensor_table if sensor['room_id'] == room and (sensor['sensor_type'] == "Smart air house keeper" or sensor['sensor_type'] == "Temperature and humidity sensor with screen" or sensor['sensor_type'] == "Temperature and humidity sensor") ]
            print(temp_sensor_id)

            current_room_temp = room_temperature(room, sensor_table)  # Current room temperature
            print('room temp', current_room_temp)

            if radiators_id_list and not temp_sensor_id:

                room_mode_intermediate(room, radiators_id_list)

            if radiators_id_list and temp_sensor_id and not current_room_temp:

                temperature_sensor_offline(radiators_id_list)
            
            elif radiators_id_list and current_room_temp:
                
                room_radiator_schedule = radiator_schedule(room, radiators_id_list)
                room_heating_rate = heating_rate(room, radiators_id_list)
                room_cooling_rate = cooling_rate(room, radiators_id_list)
        
                secondary_thu_result = secondary_thu(room)
                
                if secondary_thu_result is not None:
                
                    is_daylight = secondary_thu_result['daylight']['is_daylight']
                    max_passed = secondary_thu_result['max_passed']
                    temp_diff = secondary_thu_result['temp_diff']
                    room_trans_coef = secondary_thu_result['room_trans_coef']
                    if is_daylight and not max_passed:
                    
                        if temp_diff > 0 and room_trans_coef > 0:
                            secondary_thu_offset = round(temp_diff*room_trans_coef, 1)
        
                        else: secondary_thu_offset = 0
        
                    else: secondary_thu_offset = 0
        
                else: secondary_thu_offset = 0
        
                print('secondary_thu_offset', secondary_thu_offset)
            
                # Parse the raw schedule
                parsed_schedule = parse_schedule(room_radiator_schedule)
                print('Parsed Schedule:', parsed_schedule)
                # Get the timezone for Germany
                local_tz = pytz.timezone('Europe/Berlin')
                current_time = datetime.now(local_tz)  # This will get the current local time
                current_time = current_time.replace(tzinfo=None)
                print(f"Current time: {current_time}")
                current_room_temp = room_temperature(room, sensor_table)  # Current room temperature
                print('room temp', current_room_temp)
                room_radiator_status = radiator_status(radiators_id_list[0])
                print(room_radiator_status)
                #is_heating = True if room_radiator_status['running_state'] == 'heat' else False  # Radiator running state

                control_radiator(room, current_room_temp, radiators_id_list, parsed_schedule, current_time, room_heating_rate, room_cooling_rate, secondary_thu_offset)
                
            else: continue

while True:
    heating_management()