import paho.mqtt.client as mqtt
import json
from influxdb import InfluxDBClient
import time

# MQTT broker settings
MQTT_BROKER = "100.92.127.116" # Change to your MQTT broker's address if not localhost
MQTT_PORT = 1883  # Change if your MQTT broker uses a different port
MQTT_USERNAME = 'ga_zigbee2mqtt'  # Add your MQTT username if needed
MQTT_PASSWORD = 'ga_zigbee2mqtt'  # Add your MQTT password if needed

# Topic to get the list of devices
TOPIC_DEVICES = 'zigbee2mqtt/bridge/devices'

# InfluxDB settings
INFLUXDB_HOST = "100.92.127.116" #change to localhost
INFLUXDB_PORT = 8086
INFLUXDB_USERNAME = 'ga_influx_admin'
INFLUXDB_PASSWORD = "8666347a1afc7e596359bd01369ee00e" #"e300d7432345d75c3d077d9b92046e63"
INFLUXDB_DATABASE = 'gd_data'

# InfluxDB client setup
client_influx = InfluxDBClient(
    host=INFLUXDB_HOST,
    port=INFLUXDB_PORT,
    username=INFLUXDB_USERNAME,
    password=INFLUXDB_PASSWORD,
    database=INFLUXDB_DATABASE
)

# Create the database if it doesn't exist
client_influx.create_database(INFLUXDB_DATABASE)

# Function to insert data into InfluxDB
def save_to_influxdb(friendly_name, ieee_address, sensor_type):  # add area if available
    json_body = [
        {
            "measurement": "sensor_id",
            "tags": {
                #"area": area,
                "sensor_type": sensor_type
            },
            "fields": {
                "ieee_address": ieee_address,
                "device_name": friendly_name,
                "sensor_type": sensor_type
            }
        }
    ]
    client_influx.write_points(json_body)
    
# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with result code {rc}")
    # Subscribe to the topic where device information is published
    client.subscribe(TOPIC_DEVICES)
    # Publish a request to get the devices list
    client.publish('zigbee2mqtt/bridge/request/device/list')

# Callback when a message is received from the MQTT broker
def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8')
    devices = json.loads(payload)
    print(f"Received device list from {msg.topic}:")
    for device in devices:
        ieee_address = device['ieee_address']
        friendly_name = device.get('friendly_name', 'Unknown')
        type_of_device = device.get('type', 'Unknown')  # EndDevice, Router, etc.
        model = device.get('model', 'Unknown')  # Model of the sensor (helps to identify it)
        vendor = device.get('vendor', 'Unknown')  # Vendor/manufacturer of the sensor

        # Ensure 'definition' exists before accessing it
        definition = device.get('definition', None)
        if definition is None:
            print(f" - Device ID: {ieee_address}, Name: {friendly_name}, Description: No definition available")
            continue
        
        # Extract the description safely
        try:
            sensor_type = definition.get('description', 'No description available')
        except AttributeError as e:
            # Catch the case where 'definition' is not a dict
            print(f"Error accessing description for device {ieee_address}: {str(e)}")
            sensor_type = 'No description available'

        # Save data to InfluxDB
        save_to_influxdb(friendly_name, ieee_address, sensor_type) # add area if available
        
        print(f" - Device ID: {ieee_address}, Name: {friendly_name}, Sensor Type: {sensor_type}")
        #print(json.dumps(device, indent=4))  # Pretty print the full device structure

# Usage
if __name__ == "__main__":
    # Create MQTT client instance
    client = mqtt.Client()

    # Set username and password if needed
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    # Set the connection and message callbacks
    client.on_connect = on_connect
    client.on_message = on_message

    # Connect to the MQTT broker
    client.connect(MQTT_BROKER, MQTT_PORT, 60)

    # Start the MQTT loop to handle communication
    client.loop_start()

    # Sleep for a few seconds to allow the messages to be processed
    import time
    time.sleep(10)

    # Stop the loop and disconnect
    client.loop_stop()
    client.disconnect()

    print("Data successfully logged into InfluxDB")

    # Close the InfluxDB connection
    client_influx.close()
