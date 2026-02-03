import paho.mqtt.client as mqtt
import time
import json
import influxdb

INFLUXDB_HOST = '100.112.33.121'
MQTT_HOST = '100.112.33.121'
MQTT_PORT = 1883  # Change if your MQTT broker uses a different port
MQTT_USERNAME = 'ga_zigbee2mqtt'  # Add your MQTT username if needed
MQTT_PASSWORD = 'ga_zigbee2mqtt'
INFLUXDB_PORT = 8086
INFLUXDB_USERNAME = 'ga_influx_admin'
INFLUXDB_PASSWORD = '81dac68bbbc32f7cd174e487d17b6272'
INFLUXDB_DATABASE = 'ga_homeassistant_db'
INFLUXDB_GD_DATABASE = "gd_data"

temperature_profile = {
    "working_days": {
        "livingroom": {"00:00": 18, "06:00": 19, "08:00": 18, "17:00": 20},
        "livingroom2": {"00:00": 18, "06:00": 19, "08:00": 18, "17:00": 20},
        "bedroom": {"00:00": 17, "06:00": 18, "09:00": 18, "20:00": 18},
        "bathroom": {"00:00": 17, "06:00": 21, "08:00": 18, "17:00": 20},
        "kitchen": {"00:00": 17, "06:00": 20, "08:00": 18, "17:00": 20},
        "flur": {"00:00": 17, "06:00": 18, "08:00": 17, "17:00": 18},
        "corridor": {"00:00": 17, "06:00": 18, "08:00": 17, "17:00": 18},
        "kidsroom": {"00:00": 18, "06:00": 19, "08:00": 18, "17:00": 20}
    },
    "weekends": {
        "livingroom": {"00:00": 18, "06:00": 18, "12:00": 20, "17:00": 20},
        "livingroom2": {"00:00": 18, "06:00": 18, "12:00": 20, "17:00": 20},
        "bedroom": {"00:00": 17, "06:00": 18, "09:00": 18, "20:00": 18},
        "bathroom": {"00:00": 17, "06:00": 21, "08:00": 20, "17:00": 20},
        "kitchen": {"00:00": 17, "06:00": 20, "08:00": 20, "17:00": 20},
        "flur": {"00:00": 17, "06:00": 18, "08:00": 18, "17:00": 18},
        "corridor": {"00:00": 17, "06:00": 18, "08:00": 18, "17:00": 18},
        "kidsroom": {"00:00": 18, "06:00": 18, "12:00": 20, "17:00": 20}
    }
}

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


def on_connect_radiator(mqtt_client, userdata, flags, rc, request_topic, request_payload):
    print(f"Connected to MQTT broker with result code {rc}")

    # Publish the request to get the current state
    mqtt_client.publish(request_topic, json.dumps(request_payload))
    print(f"Published state request to {request_topic}")

# Callback when a message is received from the broker
def on_message_radiator(client, userdata, msg):
    print(f"Received message from {msg.topic}: {msg.payload.decode()}")


def get_weekly_schedule(room_name):
    # Clean up the room name
    clean_name = room_name.lower().replace(" ", "").replace("_", "")
    
    # Check if room exists
    if clean_name not in temperature_profile["working_days"]:
        available = list(temperature_profile["working_days"].keys())
        print(f"Room '{room_name}' not found. Available: {available}")
        return None
    
    # Build weekly schedule
    weekly_schedule = {}
    
    # Weekdays (Monday-Friday)
    weekdays = ["monday", "tuesday", "wednesday", "thursday", "friday"]
    for day in weekdays:
        # Sort times and format as "HH:MM/temp"
        times = sorted(temperature_profile["working_days"][clean_name].items())
        weekly_schedule[day] = " ".join([f"{t}/{temp}" for t, temp in times])
    
    # Weekends
    weekends = ["saturday", "sunday"]
    for day in weekends:
        times = sorted(temperature_profile["weekends"][clean_name].items())
        weekly_schedule[day] = " ".join([f"{t}/{temp}" for t, temp in times])
    
    return {"weekly_schedule": weekly_schedule}

sensor_table = sensors_table(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)

print(sensor_table)
rooms_id =  set(sensor['room_id'] for sensor in sensor_table if sensor['room_id'] != 'OUT')
print(rooms_id)
    
for room in rooms_id:

    print('room', room)

    weekly_format = get_weekly_schedule(room)
    
    if weekly_format is None:
        print(f"No schedule found for room {room}, skipping...")
        continue
    
    print("Schedule dict:", weekly_format)
    print("Schedule JSON:", json.dumps(weekly_format))
    
    # Get radiator IDs for this room
    radiators_id_list = [sensor['sensor_id'] for sensor in sensor_table 
                         if sensor['room_id'] == room and 
                         sensor['sensor_type'] == "Zigbee thermostatic radiator valve"]
    
    print("Radiators:", radiators_id_list)
    
    for radiator in radiators_id_list:
        MQTT_TOPIC = 'zigbee2mqtt/' + radiator + '/set'
        print('MQTT_TOPIC', MQTT_TOPIC)
        
        # PAYLOAD is the DICTIONARY, not a JSON string!
        PAYLOAD = weekly_format
        print('PAYLOAD (dict):', PAYLOAD)
        
        # Create MQTT client instance
        mqtt_client = mqtt.Client()
        
        # Set username and password if needed
        if MQTT_USERNAME and MQTT_PASSWORD:
            mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        
        # Set the connection callback
        mqtt_client.on_connect = lambda client, userdata, flags, rc: on_connect_radiator(
            client, userdata, flags, rc, MQTT_TOPIC, PAYLOAD
        )
        mqtt_client.on_message = on_message_radiator
        
        try:
            # Connect to the MQTT broker
            mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
            
            # Start the MQTT loop to handle incoming messages
            mqtt_client.loop_start()
            
            time.sleep(2)  # Give time for connection and publish
            
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
            
        except Exception as e:
            print(f"Error with radiator {radiator}: {e}")