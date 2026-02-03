import time
import threading
from typing import Dict, List
import influxdb
import pandas as pd
import paho.mqtt.client as mqtt
import time
import json
import atexit
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

INFLUXDB_HOST = '100.87.234.87'
MQTT_HOST = '100.87.234.87'
INFLUXDB_PORT = 8086
INFLUXDB_USERNAME = 'ga_influx_admin'
INFLUXDB_PASSWORD = '6ae7d195f93434099e9626b30c7c9a37'
INFLUXDB_DATABASE = 'ga_homeassistant_db'
INFLUXDB_GD_DATABASE = "gd_data"
MQTT_PORT = 1883  # Change if your MQTT broker uses a different port
MQTT_USERNAME = 'ga_zigbee2mqtt'  # Add your MQTT username if needed
MQTT_PASSWORD = 'ga_zigbee2mqtt'
# Global dictionary to track monitoring state for each room
room_monitoring_state = {}

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


def start_temperature_boost_monitoring(rooms: list):
    """
    Start monitoring room temperature and control radiator mode for multiple rooms.
    When triggered, sets radiator to manual mode 2°C above current temperature.
    When temperature increases by 1°C, switches back to AI mode.
    
    Args:
        rooms: List of room IDs to monitor, e.g. ["livingroom", "bedroom", "kitchen"]
    """
    
    def monitor_room_temperature(room_id: str):
        """Inner function to monitor temperature for a specific room"""

        try:
        
            logger.info(f"Starting temperature monitoring for room: {room_id}")
            
            # Get sensor data
            sensor_table = sensors_table(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)
            
            # Get radiator and temperature sensor IDs
            radiator_sensor_ids = [sensor['sensor_id'] for sensor in sensor_table 
                                if sensor['room_id'] == room_id 
                                and sensor['sensor_type'] == "Zigbee thermostatic radiator valve" 
                                and sensor['room_type'] == 'Active']
            
            temp_sensor_ids = [sensor['sensor_id'] for sensor in sensor_table 
                            if sensor['room_id'] == room_id 
                            and (sensor['sensor_type'] == "Smart air house keeper" 
                                or sensor['sensor_type'] == "Temperature and humidity sensor with screen" 
                                or sensor['sensor_type'] == "Temperature and humidity sensor") 
                            and sensor['room_type'] == 'Active']
            
            if not temp_sensor_ids or not radiator_sensor_ids:
                print(f"No temperature sensors or radiators found for room {room_id}")
                room_monitoring_state[room_id] = {'monitoring': False}
                return
            
            # Get initial room temperature
            client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, 
                                                username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD, 
                                                database=INFLUXDB_DATABASE)
            
            entity_id = temp_sensor_ids[0] + '_temperature'
            query_room_temp = f'SELECT LAST("value") FROM {INFLUXDB_DATABASE}."autogen"."°C" WHERE "entity_id"=\'{entity_id}\''
            room_temp_result = client_influx.query(query_room_temp)
            room_temp_data = list(room_temp_result.get_points())
            
            if not room_temp_data:
                print(f"No temperature data found for room {room_id}")
                room_monitoring_state[room_id] = {'monitoring': False}
                return
            
            initial_temperature = room_temp_data[0]['last']
            # Round to nearest .0 or .5
            rounded_initial = round(initial_temperature * 2) / 2
            target_temperature = rounded_initial + 2.0  # 2 degrees above rounded current
            switch_back_temperature = initial_temperature + 1.0  # Switch back after 1 degree increase
            
            print(f"Room {room_id}: Initial temp: {initial_temperature}°C (rounded: {rounded_initial}°C), Target: {target_temperature}°C, Switch back at: {switch_back_temperature}°C")
            
            # Record start time
            start_time = time.time()
            
            # Set all radiators in room to manual mode with target temperature
            for radiator_id in radiator_sensor_ids:
                set_radiator_manual_mode(radiator_id, target_temperature)
            
            # Store monitoring state
            room_monitoring_state[room_id] = {
                'monitoring': True,
                'initial_temperature': initial_temperature,
                'rounded_initial': rounded_initial,
                'target_temperature': target_temperature,
                'switch_back_temperature': switch_back_temperature,
                'radiator_ids': radiator_sensor_ids,
                'temp_sensor_id': temp_sensor_ids[0],
                'start_time': start_time,
                'end_time': None,
                'final_temperature': None,
                'duration_seconds': None
            }
            
            # Start monitoring loop
            while room_monitoring_state.get(room_id, {}).get('monitoring', False):
                try:
                    # Get current room temperature
                    current_temp = get_current_room_temperature(temp_sensor_ids[0])
                    
                    if current_temp is not None:
                        print(f"Room {room_id} current temperature: {current_temp}°C")
                        
                        # Check if temperature has increased by 1 degree
                        if current_temp >= switch_back_temperature:
                            end_time = time.time()
                            duration_seconds = end_time - start_time
                            
                            print(f"Room {room_id} temperature reached {current_temp}°C, switching back to AI mode")
                            
                            # Switch all radiators back to AI mode
                            for radiator_id in radiator_sensor_ids:
                                set_radiator_ai_mode(radiator_id)
                            
                            # Update monitoring state with final data
                            room_monitoring_state[room_id].update({
                                'monitoring': False,
                                'end_time': end_time,
                                'final_temperature': current_temp,
                                'duration_seconds': duration_seconds
                            })
                            
                            # Print report for this room
                            print_room_report(room_id)
                            
                            print(f"Stopped monitoring for room {room_id}")
                            break
                    
                    # Wait before next check
                    time.sleep(30)  # Check every 30 seconds
                    
                except Exception as e:
                    print(f"Error monitoring room {room_id}: {e}")
                    time.sleep(30)
            
            print(f"Temperature monitoring completed for room {room_id}")

        except Exception as e:
            try:
                print(f"Error in monitoring thread for {room_id}: {e}")
            except:
                pass 

    # Start monitoring for each room in a separate thread
    started_rooms = []
    skipped_rooms = []
    threads = []
    
    for room in rooms:
        if room in room_monitoring_state and room_monitoring_state[room].get('monitoring', False):
            print(f"Room {room} is already being monitored, skipping...")
            skipped_rooms.append(room)
            continue
        
        monitoring_thread = threading.Thread(target=monitor_room_temperature, args=(room,), daemon=False)
        monitoring_thread.start()
        threads.append(monitoring_thread)
        started_rooms.append(room)
        print(f"Started monitoring thread for room: {room}")
        time.sleep(0.5)  # Small delay between starting threads
    
    if started_rooms:
        print(f"Successfully started monitoring for {len(started_rooms)} room(s): {', '.join(started_rooms)}")
    if skipped_rooms:
        print(f"Skipped {len(skipped_rooms)} room(s) already being monitored: {', '.join(skipped_rooms)}")
    
    # Return thread references so they can be kept alive if needed
    return threads

def cleanup_monitoring():
    """Stop all monitoring threads on program exit"""
    for room in list(room_monitoring_state.keys()):
        room_monitoring_state[room] = {'monitoring': False}
    time.sleep(1)

atexit.register(cleanup_monitoring)

def set_radiator_manual_mode(radiator_id: str, target_temperature: float):
    """Set radiator to manual mode with specific target temperature"""
    
    MQTT_TOPIC = f'zigbee2mqtt/{radiator_id}/set'
    PAYLOAD = {
        "occupied_heating_setpoint": target_temperature,
        "system_mode": "heat"
        
    }
    
    print(f"Setting radiator {radiator_id} to manual mode with target {target_temperature}°C")
    
    try:
        # Create MQTT client and send command
        mqtt_client = mqtt.Client()
        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        
        connected = False
        
        def on_connect(client, userdata, flags, rc):
            nonlocal connected
            if rc == 0:
                connected = True
                client.publish(MQTT_TOPIC, json.dumps(PAYLOAD))
                print(f"Command sent to {MQTT_TOPIC}")
        
        mqtt_client.on_connect = on_connect
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        mqtt_client.loop_start()
        
        # Wait for connection and publish
        timeout = 5
        start_time = time.time()
        while not connected and (time.time() - start_time) < timeout:
            time.sleep(0.1)
        
        time.sleep(1)  # Give time for publish to complete
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        
        if not connected:
            print(f"Warning: Failed to connect to MQTT for {radiator_id}")
            
    except Exception as e:
        print(f"Error setting radiator {radiator_id} to manual mode: {e}")

def set_radiator_ai_mode(radiator_id: str):
    """Set radiator back to AI (auto) mode"""
    
    MQTT_TOPIC = f'zigbee2mqtt/{radiator_id}/set'
    PAYLOAD = {
        "system_mode": "auto"
    }
    
    print(f"Setting radiator {radiator_id} back to AI mode")
    
    try:
        # Create MQTT client and send command
        mqtt_client = mqtt.Client()
        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        
        connected = False
        
        def on_connect(client, userdata, flags, rc):
            nonlocal connected
            if rc == 0:
                connected = True
                client.publish(MQTT_TOPIC, json.dumps(PAYLOAD))
                print(f"AI mode command sent to {MQTT_TOPIC}")
        
        mqtt_client.on_connect = on_connect
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        mqtt_client.loop_start()
        
        # Wait for connection and publish
        timeout = 5
        start_time = time.time()
        while not connected and (time.time() - start_time) < timeout:
            time.sleep(0.1)
        
        time.sleep(1)  # Give time for publish to complete
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        
        if not connected:
            print(f"Warning: Failed to connect to MQTT for {radiator_id}")
            
    except Exception as e:
        print(f"Error setting radiator {radiator_id} to AI mode: {e}")

def get_current_room_temperature(temp_sensor_id: str) -> float:
    """Get current temperature from temperature sensor"""
    
    client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT,
                                          username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD,
                                          database=INFLUXDB_DATABASE)
    
    entity_id = temp_sensor_id + '_temperature'
    query_room_temp = f'SELECT LAST("value") FROM {INFLUXDB_DATABASE}."autogen"."°C" WHERE "entity_id"=\'{entity_id}\''
    
    try:
        room_temp_result = client_influx.query(query_room_temp)
        room_temp_data = list(room_temp_result.get_points())
        
        if room_temp_data:
            return room_temp_data[0]['last']
        else:
            return None
    except Exception as e:
        print(f"Error getting temperature for sensor {temp_sensor_id}: {e}")
        return None

def stop_room_monitoring(room: str):
    """Stop monitoring a specific room"""
    if room in room_monitoring_state:
        room_monitoring_state[room] = {'monitoring': False}
        print(f"Stopped monitoring for room {room}")

def stop_all_monitoring():
    """Stop monitoring for all rooms"""
    rooms_stopped = []
    for room in list(room_monitoring_state.keys()):
        if room_monitoring_state[room].get('monitoring', False):
            room_monitoring_state[room] = {'monitoring': False}
            rooms_stopped.append(room)
    
    if rooms_stopped:
        print(f"Stopped monitoring for {len(rooms_stopped)} room(s): {', '.join(rooms_stopped)}")
    else:
        print("No rooms were being monitored")

def get_monitoring_status() -> Dict:
    """Get current monitoring status for all rooms"""
    return room_monitoring_state

def print_room_report(room_id: str):
    """Print a detailed report for a specific room"""
    if room_id not in room_monitoring_state:
        print(f"No monitoring data available for room {room_id}")
        return
    
    data = room_monitoring_state[room_id]
    
    print("\n" + "="*60)
    print(f"TEMPERATURE BOOST REPORT - {room_id.upper()}")
    print("="*60)
    print(f"Initial Temperature:     {data.get('initial_temperature', 'N/A'):.2f}°C")
    print(f"Rounded Initial:         {data.get('rounded_initial', 'N/A'):.2f}°C")
    print(f"Target Temperature:      {data.get('target_temperature', 'N/A'):.2f}°C")
    print(f"Switch Back Threshold:   {data.get('switch_back_temperature', 'N/A'):.2f}°C")
    print(f"Final Temperature:       {data.get('final_temperature', 'N/A'):.2f}°C")
    
    if data.get('duration_seconds'):
        duration_minutes = data['duration_seconds'] / 60
        duration_hours = duration_minutes / 60
        print(f"Duration:                {duration_minutes:.1f} minutes ({duration_hours:.2f} hours)")
    
    print(f"Radiators Controlled:    {len(data.get('radiator_ids', []))}")
    for idx, rad_id in enumerate(data.get('radiator_ids', []), 1):
        print(f"  {idx}. {rad_id}")
    
    print(f"Temperature Sensor:      {data.get('temp_sensor_id', 'N/A')}")
    print("="*60 + "\n")

def print_all_rooms_report():
    """Print a comprehensive report for all monitored rooms"""
    completed_rooms = {room: data for room, data in room_monitoring_state.items() 
                      if not data.get('monitoring', False) and data.get('end_time') is not None}
    
    if not completed_rooms:
        print("\nNo completed monitoring sessions to report.")
        return
    
    print("\n" + "="*80)
    print("TEMPERATURE BOOST MONITORING - SUMMARY REPORT")
    print("="*80)
    print(f"Total Rooms Monitored: {len(completed_rooms)}")
    print("-"*80)
    
    for room_id, data in completed_rooms.items():
        duration_minutes = data.get('duration_seconds', 0) / 60
        print(f"\n{room_id.upper():20s} | "
              f"Initial: {data.get('initial_temperature', 0):.1f}°C → "
              f"Final: {data.get('final_temperature', 0):.1f}°C | "
              f"Duration: {duration_minutes:.1f} min | "
              f"Radiators: {len(data.get('radiator_ids', []))}")
    
    print("\n" + "="*80)
    print("\nDetailed Reports:")
    print("="*80)
    
    for room_id in completed_rooms.keys():
        print_room_report(room_id)

# Usage examples:
# Monitor multiple rooms at once
start_temperature_boost_monitoring(["livingroom", "bedroom", "kitchen", "kidsroom"])

# Monitor a single room (still works with a list)
# start_temperature_boost_monitoring(["livingroom"])

# Stop monitoring for a specific room
# stop_room_monitoring("bedroom")

# Stop all monitoring
# stop_all_monitoring()

# Check monitoring status
# status = get_monitoring_status()
# print(status)

# Print report for a specific room
# print_room_report("kitchen")

# Print comprehensive report for all rooms
# print_all_rooms_report()