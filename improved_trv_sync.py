import paho.mqtt.client as mqtt
import json
import time
from typing import Dict, List, Set
import influxdb
import logging
from dotenv import load_dotenv
import os
import sys
import pytz
#from utils import env_loader

# Add the parent directory to Python path (C:\)
current_dir = os.path.dirname(os.path.abspath(__file__))
app_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(app_dir)
sys.path.append(project_root)

# try:
#     from app.utils.sensors_table import sensors_table
#     print("✅ Import successful!")
# except ImportError as e:
#     print("❌ Import failed:", e)
#     sys.exit(1)

INFLUXDB_HOST = '100.72.87.125'
MQTT_HOST = '100.72.87.125'
MQTT_PORT = 1883  # Change if your MQTT broker uses a different port
MQTT_USERNAME = 'ga_zigbee2mqtt'  # Add your MQTT username if needed
MQTT_PASSWORD = 'ga_zigbee2mqtt'
INFLUXDB_PORT = 8086
INFLUXDB_USERNAME = 'ga_influx_admin'
INFLUXDB_PASSWORD = 'root'
INFLUXDB_DATABASE = 'ga_homeassistant_db'
INFLUXDB_GD_DATABASE = "gd_data"
TIMEZONE = 'Europe/Berlin'
TIMESTAMP_COL = 'time'
threshold = 100
LOCAL_TZ = pytz.timezone('Europe/Berlin')
extra_coef_factor = 1.5
extra_preheating_factor = 0.1
extra_factor = 0.0
DEFAULT_HEATING_RATE = 1.0
DEFAULT_COOLING_RATE = 0.0
early_stop = 0.3

# InfluxDB configurations
# INFLUXDB_HOST = os.getenv('INFLUXDB_HOST')
# INFLUXDB_PORT = os.getenv('INFLUXDB_PORT')
# INFLUXDB_USERNAME = os.getenv('INFLUXDB_USERNAME')
# INFLUXDB_PASSWORD = os.getenv('INFLUXDB_PASSWORD')
# INFLUXDB_DATABASE = os.getenv('INFLUXDB_DATABASE')
# INFLUXDB_GD_DATABASE = os.getenv('INFLUXDB_GD_DATABASE')

# # MQTT configurations
# MQTT_HOST = os.getenv('MQTT_HOST')
# print(MQTT_HOST)
# MQTT_PORT = int(os.getenv('MQTT_PORT'))
# MQTT_USERNAME = os.getenv('MQTT_USERNAME')
# MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def database_exists(client, dbname):
    existing_databases = client.get_list_database()
    return any(db['name'] == dbname for db in existing_databases)

def connect_or_create_database(host, port, user, password, dbname):
    client = influxdb.InfluxDBClient(host, port, user, password)
    if database_exists(client, dbname):
        print(f"Database '{dbname}' already exists.")
    else:
        client.create_database(dbname)
        print(f"Database '{dbname}' created successfully.")
    return influxdb.InfluxDBClient(host, port, user, password, dbname)
    
def sensors_table(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USER, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE):
    try:
        client_influx = connect_or_create_database(INFLUXDB_HOST,
                                       INFLUXDB_PORT,
                                       INFLUXDB_USER,
                                       INFLUXDB_PASSWORD,
                                       INFLUXDB_GD_DATABASE)

        query1 = 'SELECT "room_id" , "sensor_id", "floor", "device_uuid", "room_type", "sensor_location" FROM "gd_data"."autogen"."sensor_info"'
        result1 = client_influx.query(query1)

        query2 = 'SELECT "sensor_type", "ieee_address", "device_name" FROM "gd_data"."autogen"."sensor_id"'
        result2 = client_influx.query(query2)

        points1 = list(result1.get_points())
        points2 = list(result2.get_points())

        client_influx.close()

        joined_data = []
        points2_dict = {point['ieee_address']: point for point in points2}

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
        
        logger.info(f"✅ Successfully loaded {len(joined_data)} sensors from InfluxDB")
        return joined_data
    
    except Exception as e:
        logger.error(f"❌ Error loading sensor table: {e}")
        raise

class MultiTRVSyncMonitor:
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        
        sensor_table = sensors_table(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)
        
        self.sensor_table = sensor_table
        
        self.client = None
        self.connected = False
        self.reconnect_delay = 5
        self.max_reconnect_delay = 300
        
        self.last_modes = {}
        self.last_temps = {}
        self.last_print_time = {}
        
        # Track recently synced values to prevent echo loops
        # Format: device_id -> {'mode': value, 'temp': value, 'timestamp': time}
        self.recently_synced = {}
        self.sync_grace_period = 1.0  # seconds
        
        # Build room groups from sensor table
        self.room_groups = self._build_room_groups(sensor_table)
        
        # Create reverse mapping for quick lookup
        self.device_to_room = {}
        for room, devices in self.room_groups.items():
            for device in devices:
                self.device_to_room[device] = room
        
        logger.info("🎯 Multi-TRV Sync Monitor initialized")
        logger.info(f"📍 Monitoring {len(self.room_groups)} rooms")
        for room, devices in self.room_groups.items():
            if len(devices) > 1:
                logger.info(f"   {room}: {len(devices)} TRVs")
        
        self._connect()
    
    def _connect(self):
        """Connect to MQTT broker with error handling"""
        try:
            if self.client is not None:
                try:
                    self.client.disconnect()
                except:
                    pass
            
            self.client = mqtt.Client()
            self.client.username_pw_set(self.username, self.password)
            self.client.on_connect = self.on_connect
            self.client.on_message = self.on_message
            self.client.on_disconnect = self.on_disconnect
            
            logger.info(f"🔌 Attempting to connect to MQTT broker at {self.host}:{self.port}...")
            self.client.connect(self.host, self.port, 60)
            self.client.loop_start()
            self.connected = True
            self.reconnect_delay = 5
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to MQTT: {e}")
            self.connected = False
            self._schedule_reconnect()
    
    def _schedule_reconnect(self):
        """Schedule reconnection attempt"""
        logger.warning(f"⏳ Will retry connection in {self.reconnect_delay} seconds...")
        time.sleep(self.reconnect_delay)
        self.reconnect_delay = min(self.reconnect_delay * 1.5, self.max_reconnect_delay)
        self._connect()
    
    def _build_room_groups(self, sensor_table) -> Dict[str, List[str]]:
        """Build room groups from sensor table"""
        room_groups = {}
        
        rooms_id = set(sensor['room_id'] for sensor in sensor_table if sensor['room_id'] != 'OUT')
        
        for room in rooms_id:
            radiators_id_list = [sensor['sensor_id'] for sensor in sensor_table 
                               if sensor['room_id'] == room and 
                               sensor['sensor_type'] == "Zigbee thermostatic radiator valve"]
            
            if len(radiators_id_list) > 1:
                room_groups[room] = radiators_id_list
        
        return room_groups
    
    def on_connect(self, client, userdata, flags, rc):
        """Handle MQTT connection"""
        if rc == 0:
            logger.info("✅ Connected to MQTT broker")
            client.subscribe("zigbee2mqtt/+/#")
            self.connected = True
        else:
            logger.error(f"❌ Connection failed with code {rc}")
            self.connected = False
    
    def on_disconnect(self, client, userdata, rc):
        """Handle MQTT disconnection"""
        self.connected = False
        if rc != 0:
            logger.warning(f"⚠️  Unexpected MQTT disconnection (code {rc})")
            self._schedule_reconnect()
    
    def get_device_name(self, topic):
        """Extract device name from topic like 'zigbee2mqtt/0x7cc6b6fffe0bd910/...'"""
        try:
            return topic.split('/')[1]
        except IndexError:
            logger.error(f"Invalid topic format: {topic}")
            return None
    
    def publish_to_device(self, device_id, command: Dict):
        """Publish command to device via MQTT"""
        try:
            if not self.connected:
                logger.warning(f"⚠️  Not connected to MQTT, skipping publish to {device_id}")
                return
            
            topic = f"zigbee2mqtt/{device_id}/set"
            payload = json.dumps(command)
            self.client.publish(topic, payload)
            logger.info(f"   ➡️  Publishing to {device_id}: {payload}")
            
            # Mark this device as having just been synced
            self._mark_as_synced(device_id, command)
        except Exception as e:
            logger.error(f"❌ Error publishing to device: {e}")
    
    def _mark_as_synced(self, device_id, command):
        """Mark a device as recently synced to prevent echo loops"""
        if device_id not in self.recently_synced:
            self.recently_synced[device_id] = {}
        
        sync_info = self.recently_synced[device_id]
        sync_info['timestamp'] = time.time()
        
        if 'system_mode' in command:
            sync_info['mode'] = command['system_mode']
        if 'occupied_heating_setpoint' in command:
            sync_info['temp'] = command['occupied_heating_setpoint'] / 100.0
    
    def _is_recently_synced(self, device_id, mode, temp_display):
        """Check if this device was recently synced with these values"""
        if device_id not in self.recently_synced:
            return False
        
        sync_info = self.recently_synced[device_id]
        time_since_sync = time.time() - sync_info.get('timestamp', 0)
        
        # If too much time has passed, forget about it
        if time_since_sync > self.sync_grace_period:
            del self.recently_synced[device_id]
            return False
        
        # Check if the values match what we just synced
        mode_matches = mode == sync_info.get('mode')
        temp_matches = temp_display == sync_info.get('temp')
        
        # If either value was synced and matches, this is an echo
        if (mode is not None and mode_matches) or (temp_display is not None and temp_matches):
            logger.debug(f"   ↩️  Ignoring echo from {device_id} (recently synced)")
            return True
        
        return False
    
    def on_message(self, client, userdata, msg):
        """Handle incoming MQTT messages"""
        try:
            payload = msg.payload.decode()
            
            # Skip if payload is just a number
            if payload.replace('.', '').replace('-', '').isdigit():
                return
            
            data = json.loads(payload)
            
            # Only process dictionary payloads
            if not isinstance(data, dict):
                return
            
            # Check if this message contains relevant fields
            if not any(field in data for field in ['system_mode', 'occupied_heating_setpoint']):
                return
            
            device_id = self.get_device_name(msg.topic)
            if not device_id:
                return
            
            # Skip if device not in our monitored rooms
            if device_id not in self.device_to_room:
                return
            
            room = self.device_to_room[device_id]
            
            # Extract values
            new_mode = data.get('system_mode')
            new_temp_raw = data.get('occupied_heating_setpoint')
            
            if new_temp_raw is not None:
                new_temp_display = new_temp_raw / 100.0
            else:
                new_temp_display = None
            
            # Skip invalid modes
            if new_mode and (new_mode == {} or not isinstance(new_mode, str)):
                return
            
            # Check if this is an echo from our recent sync
            if self._is_recently_synced(device_id, new_mode, new_temp_display):
                return
            
            # Get previous values
            old_mode = self.last_modes.get(device_id)
            old_temp = self.last_temps.get(device_id)
            
            current_time = time.time()
            
            # Check what changed
            mode_changed = (old_mode != new_mode) and new_mode is not None
            temp_changed = (old_temp != new_temp_display) and new_temp_display is not None
            
            # Debounce check (2 second window per device)
            last_print = self.last_print_time.get(device_id, 0)
            recently_printed = (current_time - last_print) < 2.0
            
            # Process if anything changed
            if (mode_changed or temp_changed) and not recently_printed:
                # Update stored values
                if new_mode is not None:
                    self.last_modes[device_id] = new_mode
                if new_temp_display is not None:
                    self.last_temps[device_id] = new_temp_display
                
                self.last_print_time[device_id] = current_time
                
                # Print status
                logger.info(f"🔄 {room.upper()} - {device_id}")
                if mode_changed:
                    logger.info(f"   Mode: {old_mode or 'Unknown'} → {new_mode}")
                if temp_changed:
                    logger.info(f"   Temperature: {old_temp or 'Unknown'} → {new_temp_display}°C")
                logger.info(f"   Time: {time.strftime('%H:%M:%S')}")
                
                # Sync to peer devices
                self.sync_to_peers(device_id, room, mode_changed, temp_changed, new_mode, new_temp_raw)
                logger.info("   " + "─" * 40)
                    
        except (json.JSONDecodeError, UnicodeDecodeError, AttributeError, TypeError) as e:
            logger.debug(f"Error processing message: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error in on_message: {e}")

    def sync_to_peers(self, source_device, room, mode_changed=False, temp_changed=False, new_mode=None, new_temp_raw=None):
        """Sync changes to other devices in the same room"""
        try:
            peer_devices = [d for d in self.room_groups[room] if d != source_device]
            
            if not peer_devices:
                return
            
            try:
                for peer in peer_devices:
                    command = {}
                    
                    if mode_changed and new_mode is not None:
                        command['system_mode'] = new_mode
                    
                    if temp_changed and new_temp_raw is not None:
                        command['occupied_heating_setpoint'] = new_temp_raw
                    
                    if command:
                        self.publish_to_device(peer, command)
                        logger.info(f"   🔗 Syncing {room}: {source_device} → {peer}")
            except Exception as e:
                logger.error(f"❌ Error in sync_to_peers: {e}")
        
        except Exception as e:
            logger.error(f"❌ Error in sync_to_peers: {e}")


def trv_sync():
    """Main function to start the Multi-TRV Sync Monitor"""
    try:
        logger.info("=" * 50)
        logger.info("Starting Multi-TRV Sync Monitor...")
        
        # Initialize monitor
        monitor = MultiTRVSyncMonitor(MQTT_HOST, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD)
        
        # Keep the main loop running
        while True:
            time.sleep(10)
            # Optionally check connection status
            if not monitor.connected:
                logger.warning("⚠️  MQTT connection lost, attempting reconnect...")
                monitor._connect()
    
    except KeyboardInterrupt:
        logger.info("✋ Stopped by user")
    
    except Exception as e:
        logger.error(f"❌ Critical error: {e}", exc_info=True)

if __name__ == "__main__":
    trv_sync()