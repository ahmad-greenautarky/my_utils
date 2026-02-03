import asyncio
import logging
import statistics
import concurrent.futures
import functools
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, Dict
import requests
import pytz
from influxdb import InfluxDBClient
from dotenv import load_dotenv
import os
import sys
#from utils import env_loader
import influxdb

# Add the parent directory to Python path (C:\)
current_dir = os.path.dirname(os.path.abspath(__file__))
app_dir = os.path.dirname(current_dir)  # Gets the 'app' directory
project_root = os.path.dirname(app_dir)  # Gets the 'C:\' directory
sys.path.append(project_root)

# try:
#     from app.utils.sensors_table import sensors_table
#     print("✅ Import successful!")
# except ImportError as e:
#     print("❌ Import failed:", e)
#     sys.exit(1)

# INFLUXDB_HOST = os.getenv('INFLUXDB_HOST')
# INFLUXDB_PORT = os.getenv('INFLUXDB_PORT')
# INFLUXDB_USERNAME = os.getenv('INFLUXDB_USERNAME')
# INFLUXDB_PASSWORD = os.getenv('INFLUXDB_PASSWORD')
# INFLUXDB_GD_DATABASE = os.getenv('INFLUXDB_GD_DATABASE')
# DURATION = os.getenv('DURATION')
# TB_URL = os.getenv('TB_URL')
# TB_DEVICE_TOKEN = os.getenv('TB_DEVICE_TOKEN')
# DURATION = 10 # in days

INFLUXDB_HOST = '100.87.234.87'
MQTT_HOST = '100.87.234.87'
MQTT_PORT = 1883  # Change if your MQTT broker uses a different port
MQTT_USERNAME = 'ga_zigbee2mqtt'  # Add your MQTT username if needed
MQTT_PASSWORD = 'ga_zigbee2mqtt'
INFLUXDB_PORT = 8086
INFLUXDB_USERNAME = 'ga_influx_admin'
INFLUXDB_PASSWORD = '6ae7d195f93434099e9626b30c7c9a37'
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
DEFAULT_COOLING_RATE = 0.0
early_stop = 0.3
LATITUDE=52.3989
LONGITUDE=13.0657
CITY='Potsdam'
DURATION = 10  # in days
TB_URL='https://thingsboard.greenautarky.com'
TB_DEVICE_TOKEN='C0RO2E8Csu31jkTnCtL2'

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

sensors_ids = sensors_table(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)
rooms_id = set(sensor['room_id'] for sensor in sensors_ids if sensor['room_id'] != 'OUT' and sensor['room_type'] == 'Active')

hydraulic_field_prefixes = ['heating_rate', 'current_valve_opening', 'balanced_valve_opening', 'valve_adjustment', 'equilibrium_deviation', 'new_heating_rate', 'time_heat_up_1', 'new_time_heat_up_1']
HYDRAULIC_BALANCING_FIELDS = [
    f"{prefix}_{room}"
    for prefix in hydraulic_field_prefixes
    for room in rooms_id
]

SENSOR_STATUS_FIELDS = ["status", "room", "device_type"]  # Fields to fetch
AVERAGE_PD_FIELDS = ["temperature", "humidity", "co2"]
AVERAGE_PRESENCE_FIELDS = ["presence"]
OUTDOOR_WEATHER_FIELDS = ["outdoor_temperature", "outdoor_humidity"]
SENSOR_AVAILABILITY_FIELDS = ["offline_count", "online_count", "total_devices"]
HEATING_MODE_FIELDS = ["mode_auto_hours", "mode_auto_percent", "mode_heat_hours", "mode_heat_percent", "mode_off_hours", "mode_off_percent"]
HEATING_STATUS_FIELDS = ["status_idle_percent", "status_idle_hours", "status_heat_percent", "status_heat_hours"]
LQI_SUMMARY_FIELDS = ["average_lqi", "count_low_lqi", "count_good_lqi", "device_count", "alarms"]
BATTERY_SUMMARY_FIELDS = ["average_battery_level", "count_low_battery", "count_good_battery", "device_count", "alarms"]
LOAD_BALANCING_FIELDS = ["expected_demand_kW", "balanced_demand_kW"]
HEATING_RATE_FIELDS = ["heating_rate", "initial_phase_radiator_heating_rate"]
TRANSFER_COEFFICIENT_FIELDS = ["average_transfer_coefficient"]
ENERGY_CONSUMPTION_FIELDS = ["consumption_kWh"]

# Configure logging
logger = logging.getLogger(__name__)


# ==================== TIMEOUT DECORATOR ====================
def timeout_decorator(seconds=300):
    """Timeout decorator using threading"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(func, *args, **kwargs)
                try:
                    return future.result(timeout=seconds)
                except concurrent.futures.TimeoutError:
                    logger.error(f"⏰ Function '{func.__name__}' timed out after {seconds} seconds")
                    raise TimeoutError(f"Function '{func.__name__}' timed out after {seconds} seconds")
        return wrapper
    return decorator

# ==================== THINGSBOARD DATA SENDER ====================
class ThingsBoardDataSender:
    def __init__(self, timeout_seconds=300):
        self.is_running = True
        self.client = None
        self.retry_interval = 600  # 10 minutes in seconds
        self.max_retries = 144    # 24 hours worth of retries
        self.timeout_seconds = timeout_seconds
        self.query_timeout = 30  # 30 seconds per InfluxDB query

    def get_influx_client(self):
        """Create and return a new InfluxDB client instance with timeout"""
        return InfluxDBClient(
            host=INFLUXDB_HOST,
            port=INFLUXDB_PORT,
            username=INFLUXDB_USERNAME,
            password=INFLUXDB_PASSWORD,
            database=INFLUXDB_GD_DATABASE,
            timeout=self.query_timeout,
            retries=2
        )

    @timeout_decorator(seconds=30)
    def safe_query(self, query: str):
        """Execute a query with timeout protection"""
        client = self.get_influx_client()
        try:
            return client.query(query)
        finally:
            client.close()

    def fetch_and_average_pd(self) -> List[Dict]:
        """Fetch data from InfluxDB and calculate averages across rooms."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=DURATION)

            sensor_table = sensors_table(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)

            # Get active rooms (excluding OUT and Main Door)
            rooms = {s['room_id'] for s in sensor_table 
                     if s['room_id'] not in ('OUT', 'Main Door') 
                     and s['room_type'] == 'Active'}
            
            timestamp_data = defaultdict(lambda: {
                'temperature': [],
                'humidity': [],
                'co2': []
            })

            client = self.get_influx_client()
            
            for room in rooms:
                query = f'''
                    SELECT {",".join(AVERAGE_PD_FIELDS)}
                    FROM "gd_data"."autogen"."average_pd"
                    WHERE time > '{start_time.isoformat()}Z'
                    AND time <= '{end_time.isoformat()}Z'
                    AND "room" = '{room}'
                '''
                
                try:
                    result = client.query(query)
                    
                    for point in result.get_points():
                        timestamp = point['time']
                        
                        for field in ['temperature', 'humidity', 'co2']:
                            if point.get(field) is not None:
                                field_key = field.replace('mean_', '')
                                timestamp_data[timestamp][field_key].append(point[field])
                except Exception as e:
                    logger.error(f"Error querying room {room}: {str(e)}")
                    continue
            
            # Calculate averages
            averaged_results = []
            for timestamp, measurements in timestamp_data.items():
                result_point = {'time': timestamp}
                
                for field in ['temperature', 'humidity', 'co2']:
                    values = measurements[field]
                    if values:
                        result_point[f'mean_{field}'] = statistics.mean(values)
                    else:
                        result_point[f'mean_{field}'] = None
                
                averaged_results.append(result_point)
            
            averaged_results.sort(key=lambda x: x['time'])
            logger.info(f"✅ Successfully fetched and averaged {len(averaged_results)} PD data points")
            return averaged_results
            
        except Exception as e:
            logger.error(f"❌ Error in fetch_and_average_pd: {str(e)}")
            return []
        finally:
            if 'client' in locals():
                client.close()
    
    def fetch_transfer_coefficient(self) -> List[Dict]:
        """Fetch data from InfluxDB using InfluxQL."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=DURATION)

            select_clause = "last" + ", ".join(
                [f'("{field}") AS "{field}"' for field in TRANSFER_COEFFICIENT_FIELDS]
            )
            
            query = f'''
                SELECT {select_clause}
                FROM "gd_data"."autogen"."transfer_coefficient"
                WHERE time > '{start_time.isoformat()}Z'
                AND time <= '{end_time.isoformat()}Z'
            '''

            client = self.get_influx_client()
            result = client.query(query)
            
            data_points = list(result.get_points())
            logger.info(f"✅ Successfully fetched {len(data_points)} transfer coefficient data points")
            return data_points
            
        except Exception as e:
            logger.error(f"❌ Error fetching transfer coefficient data: {str(e)}")
            return []
        finally:
            if 'client' in locals():
                client.close()
    
    def fetch_sensor_availability(self) -> List[Dict]:
        """Fetch data from InfluxDB using InfluxQL."""
        try:
            query = f'''
                SELECT last("offline_count") AS "offline_count", 
                       ("online_count") AS "online_count", 
                       ("total_devices") AS "total_sensors" 
                FROM "gd_data"."autogen"."sensors_availability_summary"
            '''

            client = self.get_influx_client()
            result = client.query(query)
            data = list(result.get_points())
            logger.info(f"✅ Successfully fetched {len(data)} sensor availability data points")
            return data
        except Exception as e:
            logger.error(f"❌ Error fetching sensor availability: {str(e)}")
            return []
        finally:
            if 'client' in locals():
                client.close()

    def fetch_sensor_inventory(self) -> List[Dict]:
        """Fetch data from InfluxDB using InfluxQL."""
        try:
            query = '''
                SELECT LAST("count") AS "count", "sensor_type" 
                FROM "gd_data"."autogen"."sensor_inventory" 
                GROUP BY "sensor_type"
            '''
    
            client = self.get_influx_client()
            result = client.query(query)
            raw_data = list(result.get_points())
            logger.info(f"✅ Successfully fetched {len(raw_data)} sensor inventory data points")
            return self.prepare_sensor_inventory_data(raw_data)
        except Exception as e:
            logger.error(f"❌ Error fetching sensor inventory: {str(e)}")
            return []
        finally:
            if 'client' in locals():
                client.close()
    
    def prepare_sensor_inventory_data(self, raw_data: List[Dict]) -> List[Dict]:
        """
        Transform raw sensor data and return only entries from the latest timestamp.
        """
        if not raw_data:
            return []
            
        # Define all known sensor types
        all_sensor_types = [
            'Motion',
            'Window',
            'T&H-Display',
            'Thermostat',
            'T&H-NoDisplay',
            'Presence'
        ]
        
        # Find the latest timestamp
        latest_timestamp = max(entry['time'] for entry in raw_data)
        
        # Filter to only entries with the latest timestamp
        latest_entries = [entry for entry in raw_data if entry['time'] == latest_timestamp]
        
        # Create a single combined entry for the latest timestamp
        combined_entry = {'time': latest_timestamp}
        
        # Initialize all known sensor types to 0
        for sensor_type in all_sensor_types:
            field_name = sensor_type.lower().replace(' ', '_')
            field_name = field_name.replace('&', '_and_')
            field_name = field_name.replace('-', '_')
            combined_entry[f"count_{field_name}"] = 0
        
        # Then override with actual values from latest timestamp
        for entry in latest_entries:
            sensor_type = entry['sensor_type'].lower().replace(' ', '_')
            field_name = f"count_{sensor_type}"
            field_name = field_name.replace('&', '_and_')
            field_name = field_name.replace('-', '_')
            combined_entry[field_name] = entry['count']
        
        return [combined_entry]

    def fetch_and_average_presence(self) -> List[Dict]:
        """Fetch data from InfluxDB and calculate averages across rooms."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=500)

            sensor_table = sensors_table(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)

            # Get active rooms (excluding OUT and Main Door)
            rooms = {s['room_id'] for s in sensor_table 
                    if s['room_id'] not in ('OUT', 'Main Door') 
                    and s['room_type'] == 'Active'}
            
            # Dictionary to store all data points grouped by timestamp
            timestamp_data = defaultdict(list)

            client = self.get_influx_client()
            
            for room in rooms:
                query = f'''
                    SELECT {",".join(AVERAGE_PRESENCE_FIELDS)}
                    FROM "gd_data"."autogen"."average_pd"
                    WHERE time > '{start_time.isoformat()}Z'
                    AND time <= '{end_time.isoformat()}Z'
                    AND "room" = '{room}'
                '''
                result = client.query(query)
                
                # Process the result for this room
                for point in result.get_points():
                    timestamp = point['time']
                    timestamp_data[timestamp].append({
                        'mean_presence': point['presence']*100
                    })
            
            # Calculate averages for each timestamp
            averaged_results = []
            for timestamp, measurements in timestamp_data.items():
                # Extract all presence values for this timestamp
                presence = [m['mean_presence'] for m in measurements]
                
                averaged_results.append({
                    'time': timestamp,
                    'presence': statistics.mean(presence)
                })
            
            # Sort results by timestamp
            averaged_results.sort(key=lambda x: x['time'])
            
            logger.info(f"✅ Successfully fetched and averaged {len(averaged_results)} presence data points")
            return averaged_results
        except Exception as e:
            logger.error(f"❌ Error fetching and averaging presence data: {str(e)}")
            return []
        finally:
            if 'client' in locals():
                client.close()

    def fetch_heating_rate(self) -> List[Dict]:
        """Fetch data from InfluxDB using InfluxQL."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=DURATION)

            select_clause = ", ".join(
                [f'mean("{field}") AS "{field}"' for field in HEATING_RATE_FIELDS]
            )
            
            query = f'''
                SELECT {select_clause}
                FROM "gd_data"."autogen"."preprocessed_heating_rate"
                WHERE time > '{start_time.isoformat()}Z'
                AND time <= '{end_time.isoformat()}Z'
                fill(0)
                '''
            
            client = self.get_influx_client()
            result = client.query(query)
            data = list(result.get_points())
            logger.info(f"✅ Successfully fetched {len(data)} heating rate data points")
            return data
        except Exception as e:
            logger.error(f"❌ Error fetching heating rate: {str(e)}")
            return []
        finally:
            if 'client' in locals():
                client.close()

    def fetch_hydraulic_balancing(self) -> List[Dict]:
        """Fetch data from InfluxDB and transform for ThingsBoard."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=DURATION)
            
            query = f'''
            SELECT last("heating_rate") AS "heating_rate",
                ("current_valve_opening") AS "current_valve_opening",
                ("balanced_valve_opening") AS "balanced_valve_opening",
                ("valve_adjustment") AS "valve_adjustment",
                ("equilibrium_deviation") AS "equilibrium_deviation",
                ("new_heating_rate") AS "new_heating_rate",
                ("time_heat_up_1") AS "time_heat_up_1",
                ("new_time_heat_up_1") AS "new_time_heat_up_1"
            FROM "gd_data"."autogen"."hydraulic_balancing"
            WHERE time > '{start_time.isoformat()}Z' 
            AND time <= '{end_time.isoformat()}Z'
            GROUP BY "radiator_id", "room"
            fill(0)
            '''
            
            client = self.get_influx_client()
            result = client.query(query)
            
            transformed_data = []
            field_names = [
                'heating_rate',
                'current_valve_opening',
                'balanced_valve_opening',
                'valve_adjustment',
                'equilibrium_deviation',
                'new_heating_rate',
                'time_heat_up_1',
                'new_time_heat_up_1'
            ]
            
            # Access the raw response dict
            raw_response = result.raw if hasattr(result, 'raw') else result
            
            for series in raw_response.get('series', []):
                tags = series.get('tags', {})
                room = tags.get('room', '')
                
                if room and series.get('values'):
                    columns = series['columns']
                    values = series['values'][0]  # Get first (latest) value
                    time_idx = columns.index('time') if 'time' in columns else 0
                    
                    # Create one point per room with all fields
                    point = {
                        'time': values[time_idx]
                    }
                    
                    for field in field_names:
                        if field in columns:
                            col_idx = columns.index(field)
                            key = f"{field}_{room}"
                            point[key] = values[col_idx] if col_idx < len(values) else 0
                    
                    transformed_data.append(point)
            
            logger.info(f"✅ Successfully fetched and transformed {len(transformed_data)} hydraulic balancing records")
            print( transformed_data)
            return transformed_data
            
        except Exception as e:
            logger.error(f"❌ Error fetching hydraulic balancing: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []
        finally:
            if 'client' in locals():
                client.close()

    def fetch_weather_forecast(self) -> List[Dict]:
        """Fetch data from InfluxDB using InfluxQL."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=DURATION)

            query = f'''
                SELECT ("temperature") AS "outdoor_temperature", 
                       ("humidity") AS "outdoor_humidity"
                FROM "gd_data"."autogen"."current_weather"
                WHERE time > '{start_time.isoformat()}Z'
                AND time <= '{end_time.isoformat()}Z'
            '''
            
            client = self.get_influx_client()
            result = client.query(query)
            data = list(result.get_points())
            logger.info(f"✅ Successfully fetched {len(data)} weather forecast data points")
            return data
        except Exception as e:
            logger.error(f"❌ Error fetching weather forecast: {str(e)}")
            return []
        finally:
            if 'client' in locals():
                client.close()

    def fetch_heating_mode(self) -> List[Dict]:
        """Fetch data from InfluxDB using InfluxQL."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=DURATION)

            select_clause = ", ".join(
                [f'mean("{field}") AS "{field}"' for field in HEATING_MODE_FIELDS]
            )

            query = f'''
                SELECT {select_clause}
                FROM "gd_data"."autogen"."heating_usage"
                WHERE time > '{start_time.isoformat()}Z'
                AND time <= '{end_time.isoformat()}Z'
                GROUP BY time(7d) fill(0)
            '''
            
            print("Heating Mode Query:", query)
            client = self.get_influx_client()
            result = client.query(query)
            data = list(result.get_points())
            logger.info(f"✅ Successfully fetched {len(data)} heating mode data points")
            return data
        except Exception as e:
            logger.error(f"❌ Error fetching heating mode: {str(e)}")
            return []
        finally:
            if 'client' in locals():
                client.close()

    def fetch_heating_status(self) -> List[Dict]:
        """Fetch data from InfluxDB using InfluxQL."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=DURATION)

            select_clause = ", ".join(
                [f'mean("{field}") AS "{field}"' for field in HEATING_STATUS_FIELDS]
            )

            query = f'''
                SELECT {select_clause}
                FROM "gd_data"."autogen"."heating_usage"
                WHERE time > '{start_time.isoformat()}Z'
                AND time <= '{end_time.isoformat()}Z'
                GROUP BY time(7d) fill(0)
            '''

            print("Heating Status Query:", query)
            client = self.get_influx_client()
            result = client.query(query)
            data = list(result.get_points())
            logger.info(f"✅ Successfully fetched {len(data)} heating status data points")
            return data
        except Exception as e:
            logger.error(f"❌ Error fetching heating status: {str(e)}")
            return []
        finally:
            if 'client' in locals():
                client.close()

    def fetch_lqi_summary(self) -> List[Dict]:
        """Fetch data from InfluxDB using InfluxQL."""
        try:
            select_clause = "last" + ", ".join(
                [f'("{field}") AS "{field}"' for field in LQI_SUMMARY_FIELDS]
            )
            
            query = f'''
                SELECT {select_clause}
                FROM "gd_data"."autogen"."lqi_summary"
                '''
            
            client = self.get_influx_client()
            result = client.query(query)
            data = list(result.get_points())
            logger.info(f"✅ Successfully fetched {len(data)} LQI summary data points")
            return data
        except Exception as e:
            logger.error(f"❌ Error fetching LQI summary: {str(e)}")
            return []
        finally:
            if 'client' in locals():
                client.close()

    def fetch_battery_level_summary(self) -> List[Dict]:
        """Fetch data from InfluxDB using InfluxQL."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=DURATION)

            select_clause = "last" + ", ".join(
                [f'("{field}") AS "{field}"' for field in BATTERY_SUMMARY_FIELDS]
            )


            query = f'''
                SELECT {select_clause}
                FROM "gd_data"."autogen"."battery_summary"
                WHERE time > '{start_time.isoformat()}Z'
                AND time <= '{end_time.isoformat()}Z'
                GROUP BY time(7d) fill(0)
            '''

            client = self.get_influx_client()
            result = client.query(query)
            data = list(result.get_points())
            logger.info(f"✅ Successfully fetched {len(data)} battery level summary data points")
            return data
        except Exception as e:
            logger.error(f"❌ Error fetching battery level summary: {str(e)}")
            return []
        finally:
            if 'client' in locals():
                client.close()

    def fetch_load_balancing(self) -> List[Dict]:
        """Fetch data from InfluxDB using InfluxQL."""
        try:
            query = f'''
                SELECT last({",".join(LOAD_BALANCING_FIELDS)})
                FROM "gd_data"."autogen"."central_heating_demand"
            '''

            client = self.get_influx_client()
            result = client.query(query)
            data = list(result.get_points())
            logger.info(f"✅ Successfully fetched {len(data)} load balancing data points")
            return data
        except Exception as e:
            logger.error(f"❌ Error fetching load balancing: {str(e)}")
            return []
        finally:
            if 'client' in locals():
                client.close()

    def fetch_energy_consumption(self) -> List[Dict]:
        """Fetch data from InfluxDB using InfluxQL."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=DURATION)
            
            query = f'''
                SELECT last({",".join(ENERGY_CONSUMPTION_FIELDS)})
                FROM "gd_data"."autogen"."expected_energy_consumption"
                WHERE time > '{start_time.isoformat()}Z'
                AND time <= '{end_time.isoformat()}Z'
            '''

            client = self.get_influx_client()
            result = client.query(query)
            data = list(result.get_points())
            logger.info(f"✅ Successfully fetched {len(data)} energy consumption data points")
            return data
        except Exception as e:
            logger.error(f"❌ Error fetching energy consumption: {str(e)}")
            return []
        finally:
            if 'client' in locals():
                client.close()

    def send_to_thingsboard(self, data_points: List[Dict], fields: List[str]) -> bool:
        """Send data to ThingsBoard via HTTP API with timeout."""
        if not data_points:
            logger.warning("⚠️ No data points to send to ThingsBoard")
            return False
        
        successful_sends = 0
        total_points = len(data_points)
        
        for point in data_points:
            try:
                # Parse timestamp
                try:
                    dt = datetime.strptime(point['time'], "%Y-%m-%dT%H:%M:%S.%fZ")
                    dt = dt.replace(microsecond=0)
                except ValueError:
                    try:
                        dt = datetime.strptime(point['time'], "%Y-%m-%dT%H:%M:%SZ")
                    except ValueError as e:
                        logger.error(f"❌ Invalid time format '{point['time']}': {str(e)}")
                        continue

                # Convert to local timezone
                dt = pytz.utc.localize(dt).astimezone(pytz.timezone("Europe/Berlin"))
                ts = int(dt.timestamp() * 1000)

                payload = {
                    "ts": ts,
                    "values": {
                        field: point[field] for field in fields if field in point and point[field] is not None
                    }
                }
                
                response = requests.post(
                    f"{TB_URL}/api/v1/{TB_DEVICE_TOKEN}/telemetry",
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=10.0  # Shorter timeout for sending
                )
                
                if response.status_code == 200:
                    successful_sends += 1
                else:
                    logger.error(f"❌ Failed to send data to ThingsBoard. Status: {response.status_code}")
                    return False
                    
            except requests.exceptions.Timeout:
                logger.error("⏰ Timeout sending to ThingsBoard")
                return False
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Network error sending to ThingsBoard: {str(e)}")
                return False
            except Exception as e:
                logger.error(f"❌ Unexpected error sending data point: {str(e)}")
                return False
        
        if successful_sends == total_points:
            logger.info(f"✅ Successfully sent all {successful_sends} data points to ThingsBoard")
            return True
        else:
            logger.warning(f"⚠️ Sent {successful_sends}/{total_points} data points to ThingsBoard")
            return successful_sends > 0

    async def collect_and_send_data_fast(self) -> bool:
        """Collect only critical data types quickly"""
        try:
            logger.info("🚀 Starting fast data collection for ThingsBoard")
            
            # Only fetch critical data
            critical_operations = [
                (self.fetch_hydraulic_balancing, HYDRAULIC_BALANCING_FIELDS, "hydraulic balancing"),
                # (self.fetch_sensor_availability, SENSOR_AVAILABILITY_FIELDS, "sensor availability"),
                # (self.fetch_and_average_pd, ['mean_temperature', 'mean_humidity', 'mean_co2'], "averaged PD"),
                # (self.fetch_heating_status, HEATING_STATUS_FIELDS, "heating status"),
                # (self.fetch_battery_level_summary, BATTERY_SUMMARY_FIELDS, "battery summary"),
                # (self.fetch_lqi_summary, LQI_SUMMARY_FIELDS, "LQI summary"),
            ]
            
            success_count = 0
            
            for fetch_func, fields, operation_name in critical_operations:
                try:
                    data = fetch_func()
                    
                    if data and len(data) > 0:
                        if self.send_to_thingsboard(data, fields):
                            success_count += 1
                            logger.info(f"✅ Sent {operation_name} data")
                        else:
                            logger.warning(f"⚠️ Failed to send {operation_name} data")
                    else:
                        logger.warning(f"⚠️ No {operation_name} data available")
                        
                except Exception as e:
                    logger.error(f"❌ Error processing {operation_name} data: {str(e)}")
                    continue
            
            # Consider successful if at least 60% of critical operations succeeded
            if success_count >= len(critical_operations) * 0.6:
                logger.info(f"✅ Fast collection: {success_count}/{len(critical_operations)} operations succeeded")
                return True
            else:
                logger.warning(f"⚠️ Fast collection: Only {success_count}/{len(critical_operations)} operations succeeded")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error in fast data collection: {str(e)}")
            return False

    def run_sync_fast(self, timeout_seconds=300) -> bool:
        """Run fast data collection synchronously with timeout"""
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    lambda: asyncio.run(self.collect_and_send_data_fast())
                )
                return future.result(timeout=timeout_seconds)
        except concurrent.futures.TimeoutError:
            logger.error(f"⏰ BVibe fast collection timed out after {timeout_seconds} seconds")
            return False
        except Exception as e:
            logger.error(f"❌ Error in BVibe fast collection: {str(e)}")
            return False

    async def collect_and_send_data_full(self) -> bool:
        """Collect all data types (original full version)"""
        for attempt in range(3):  # Only 3 attempts for full collection
            try:
                logger.info(f"Attempt {attempt + 1} to fetch and send all data to ThingsBoard")
                
                success_count = 0
                total_operations = 14
                
                operations = [
                    (self.fetch_and_average_pd, ['mean_temperature', 'mean_humidity', 'mean_co2'], "averaged PD"),
                    (self.fetch_transfer_coefficient, TRANSFER_COEFFICIENT_FIELDS, "transfer coefficient"),
                    (self.fetch_sensor_availability, SENSOR_AVAILABILITY_FIELDS, "sensor availability"),
                    (self.fetch_sensor_inventory, None, "sensor inventory"),
                    (self.fetch_and_average_presence, ['presence'], "presence"),
                    (self.fetch_heating_rate, HEATING_RATE_FIELDS, "heating rate"),
                    (self.fetch_hydraulic_balancing, HYDRAULIC_BALANCING_FIELDS, "hydraulic balancing"),
                    (self.fetch_weather_forecast, OUTDOOR_WEATHER_FIELDS, "weather forecast"),
                    (self.fetch_heating_mode, HEATING_MODE_FIELDS, "heating mode"),
                    (self.fetch_heating_status, HEATING_STATUS_FIELDS, "heating status"),
                    (self.fetch_lqi_summary, LQI_SUMMARY_FIELDS, "LQI summary"),
                    (self.fetch_battery_level_summary, BATTERY_SUMMARY_FIELDS, "battery summary"),
                    (self.fetch_load_balancing, LOAD_BALANCING_FIELDS, "load balancing"),
                    (self.fetch_energy_consumption, ENERGY_CONSUMPTION_FIELDS, "energy consumption")
                ]
                
                for fetch_func, fields, operation_name in operations:
                    try:
                        data = fetch_func()
                        
                        if operation_name == "sensor inventory" and data:
                            # Get dynamic field names
                            fields = [key for key in data[0].keys() if key != 'time']
                        
                        if data and len(data) > 0:
                            if self.send_to_thingsboard(data, fields):
                                success_count += 1
                                logger.info(f"✅ Sent {operation_name} data")
                            else:
                                logger.warning(f"⚠️ Failed to send {operation_name} data")
                        else:
                            logger.warning(f"⚠️ No {operation_name} data available")
                            
                    except Exception as e:
                        logger.error(f"❌ Error processing {operation_name} data: {str(e)}")
                        continue
                
                if success_count >= total_operations // 2:
                    logger.info(f"✅ Full collection: {success_count}/{total_operations} operations succeeded")
                    return True
                else:
                    logger.warning(f"⚠️ Full collection: Only {success_count}/{total_operations} operations succeeded")
                
                if attempt < 2:
                    await asyncio.sleep(30)  # Wait 30 seconds before retry
                
            except Exception as e:
                logger.error(f"❌ Unexpected error during attempt {attempt + 1}: {str(e)}", exc_info=True)
                if attempt < 2:
                    await asyncio.sleep(30)
        
        logger.error("❌ Failed to collect and send all data to ThingsBoard after 3 attempts")
        return False

    def run_sync_full(self, timeout_seconds=600) -> bool:
        """Run full data collection synchronously with timeout (for testing)"""
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    lambda: asyncio.run(self.collect_and_send_data_full())
                )
                return future.result(timeout=timeout_seconds)
        except concurrent.futures.TimeoutError:
            logger.error(f"⏰ BVibe full collection timed out after {timeout_seconds} seconds")
            return False
        except Exception as e:
            logger.error(f"❌ Error in BVibe full collection: {str(e)}")
            return False

# ==================== PUBLIC INTERFACE ====================
def fetch_bvibe_data(timeout_seconds=300):
    """
    Legacy function that uses the new class with timeout protection.
    
    Args:
        timeout_seconds: Maximum time to allow for the operation (default: 300 seconds = 5 minutes)
    
    Returns:
        bool: True if successful, False otherwise
    """
    print("=" * 60)
    print("🚀 Starting InfluxDB to ThingsBoard batch transfer...")
    print(f"⏱️ Timeout: {timeout_seconds} seconds")
    print("=" * 60)
    
    try:
        # Use fast mode by default to prevent hanging
        sender = ThingsBoardDataSender(timeout_seconds=timeout_seconds)
        
        # Try fast collection first
        print("🔄 Attempting fast data collection (critical metrics only)...")
        result = sender.run_sync_fast(timeout_seconds=timeout_seconds)
        
        if result:
            print("✅ BVibe fast data transfer completed successfully")
            return True
        else:
            print("⚠️ Fast collection failed or incomplete")
            return False
            
    except TimeoutError as e:
        print(f"⏰ BVibe operation timed out after {timeout_seconds} seconds")
        return False
    except Exception as e:
        print(f"❌ Error in BVibe data transfer: {str(e)}")
        return False
    finally:
        print("=" * 60)

def fetch_bvibe_data_full(timeout_seconds=600):
    """
    Full data collection (for testing or manual runs)
    
    Args:
        timeout_seconds: Maximum time to allow (default: 600 seconds = 10 minutes)
    
    Returns:
        bool: True if successful, False otherwise
    """
    print("=" * 60)
    print("🚀 Starting FULL InfluxDB to ThingsBoard batch transfer...")
    print(f"⏱️ Timeout: {timeout_seconds} seconds")
    print("=" * 60)
    
    try:
        sender = ThingsBoardDataSender(timeout_seconds=timeout_seconds)
        result = sender.run_sync_full(timeout_seconds=timeout_seconds)
        
        if result:
            print("✅ BVibe FULL data transfer completed successfully")
        else:
            print("❌ BVibe FULL data transfer failed")
        
        return result
        
    except TimeoutError as e:
        print(f"⏰ BVibe FULL operation timed out after {timeout_seconds} seconds")
        return False
    except Exception as e:
        print(f"❌ Error in BVibe FULL data transfer: {str(e)}")
        return False
    finally:
        print("=" * 60)

# ==================== MAIN EXECUTION ====================
# if __name__ == "__main__":
#     """For testing the function directly"""
#     import sys
    
#     # Check for command line arguments
#     if len(sys.argv) > 1 and sys.argv[1] == "--full":
#         # Run full version
#         result = fetch_bvibe_data_full(timeout_seconds=600)
#     else:
#         # Run fast version by default
#         result = fetch_bvibe_data(timeout_seconds=300)
    
#     if result:
#         print("\n🎉 BVibe data transfer completed!")
#         sys.exit(0)
#     else:
#         print("\n💥 BVibe data transfer failed!")
#         sys.exit(1)


result = fetch_bvibe_data(timeout_seconds=300)



