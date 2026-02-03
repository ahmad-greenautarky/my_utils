import pandas as pd
from influxdb import InfluxDBClient
import logging
from sqlalchemy import create_engine 

# Set up logging
logger = logging.getLogger(__name__)

class EdgeDataProcessor:
    def __init__(self, cloud_db_config, influx_config):
        self.cloud_db_config = cloud_db_config
        self.influx_config = influx_config
        
    def get_cloud_connection(self):
        """Get connection to cloud database using SQLAlchemy"""
        connection_string = f"postgresql://{self.cloud_db_config['user']}:{self.cloud_db_config['password']}@{self.cloud_db_config['host']}/{self.cloud_db_config['database']}"
        engine = create_engine(connection_string)
        return engine
    
    def get_influx_client(self):
        """Get InfluxDB client"""
        return InfluxDBClient(
            host=self.influx_config['host'],
            port=self.influx_config['port'],
            username=self.influx_config['username'],
            password=self.influx_config['password'],
            database=self.influx_config['database']
        )
    
    def get_device_identifiers_from_influx(self):
        """Fetch MAC address and HA Instance ID from InfluxDB"""
        mac_address = None
        ha_instance_id = None
        
        try:
            client = self.get_influx_client()
            query = 'SELECT ha_instance_id, mac_address FROM device_info ORDER BY time DESC LIMIT 1'
            result = client.query(query)
            points = list(result.get_points())
            
            if points:
                point = points[0]
                ha_instance_id = point.get('ha_instance_id')
                mac_address = point.get('mac_address')
                logger.info(f"Retrieved device identifiers - MAC: {mac_address}, HA ID: {ha_instance_id}")
            else:
                logger.warning("No device_info found in InfluxDB")
            
            client.close()
        except Exception as e:
            logger.error(f"Error fetching device identifiers: {e}")
        
        return {'mac_address': mac_address, 'ha_instance_id': ha_instance_id}
    
    def fetch_filtered_sensor_data(self, device_id):
        """Fetch data from multiple tables and join them properly"""
        
        try:
            engine = self.get_cloud_connection()
            
            # Step 1: Get apartment_id from dim_apartment using device_id (ha_instance_id)
            apartment_query = """
            SELECT apartment_id
            FROM dim_apartment 
            WHERE device_id = %s
            """
            
            apartment_df = pd.read_sql_query(apartment_query, engine, params=(device_id,))
            
            if apartment_df.empty:
                logger.warning(f"No apartment found for device_id: {device_id}")
                # No need to close engine, just return
                return None
            
            apartment_id = apartment_df.iloc[0]['apartment_id']
            logger.info(f"Found apartment_id: {apartment_id} for device_id: {device_id}")
            
            # Step 2: Get basic device info from dim_device for this apartment
            device_query = """
            SELECT 
                room as room_name,
                ieee_address as sensor_id,
                sensor_type as sensor_location
            FROM dim_device 
            WHERE apartment_id = %s
            """
            
            device_df = pd.read_sql_query(device_query, engine, params=(apartment_id,))
            
            if device_df.empty:
                logger.warning(f"No devices found in dim_device for apartment_id: {apartment_id}")
                return None
            
            logger.info(f"Found {len(device_df)} devices for apartment_id: {apartment_id}")


            
            # Step 3: Get room_type from dim_room using apartment_id and room_name
            room_types = []
            for index, row in device_df.iterrows():
                room_name = row['room_name']
                room_query = """
                SELECT room_type 
                FROM dim_room 
                WHERE apartment_id = %s AND room_name = %s
                """
                
                room_df = pd.read_sql_query(room_query, engine, params=(apartment_id, room_name))
                room_type = room_df.iloc[0]['room_type'] if not room_df.empty else 'unknown'
                room_types.append(room_type)
            
            # Add room_type to device dataframe
            device_df['room_type'] = room_types
            
            # Step 4: Get floor from dim_apartment using apartment_id
            floor_query = """
            SELECT floor
            FROM dim_apartment 
            WHERE apartment_id = %s
            """
            
            floor_df = pd.read_sql_query(floor_query, engine, params=(apartment_id,))
            floor = floor_df.iloc[0]['floor'] if not floor_df.empty else 0
            
            # Add apartment_id and floor to final dataframe
            device_df['apartment_id'] = apartment_id
            device_df['floor'] = floor
            
            # Rename room_name to room_id for final output
            device_df.rename(columns={'room_name': 'room_id'}, inplace=True)
            
            # Reorder columns to match your desired structure
            final_df = device_df[['floor', 'apartment_id', 'room_id', 'room_type', 'sensor_id', 'sensor_location']]
            
            return final_df
            
        except Exception as e:
            logger.error(f"Error fetching filtered sensor data: {e}")
            return None
    
    def process_and_save_to_influx(self, csv_file_path=None):
        """Main method to process data and save to InfluxDB"""
        
        # Get device identifiers - using ha_instance_id as device_id
        device_info = self.get_device_identifiers_from_influx()
        device_id = device_info.get('ha_instance_id')  # Using HA instance ID as device_id
        device_id = 'd054161b9ec14bc988eea0e4ab56a27c'

        print(device_id)
        
        if not device_id:
            logger.error("No HA instance ID found in InfluxDB")
            return
        
        logger.info(f"Processing data for device_id (HA instance): {device_id}")
        
        # Fetch filtered data from cloud
        df = self.fetch_filtered_sensor_data(device_id)

        print(df)
        
        if df is None or df.empty:
            logger.error("No data fetched from cloud database")
            return
        
        logger.info(f"Fetched {len(df)} records from cloud database")
        
        # If CSV file is provided, you can merge additional data
        if csv_file_path:
            try:
                csv_df = pd.read_csv(csv_file_path)
                csv_df = csv_df.dropna()
                csv_df.rename({'sensor_type': 'sensor_location'}, inplace=True, axis='columns')
                
                # Merge based on sensor_id
                df = pd.merge(df, csv_df, on=['sensor_id'], how='left', suffixes=('_cloud', '_csv'))
                logger.info(f"Merged with CSV data, total records: {len(df)}")
                
            except Exception as e:
                logger.error(f"Error reading or merging CSV file: {e}")
        
        # Save to InfluxDB
        self.save_to_influxdb(df)
        
        logger.info(f"Data successfully processed and saved to InfluxDB. Total records: {len(df)}")
    
    def save_to_influxdb(self, df):
        """Save DataFrame to InfluxDB with measurement name 'sensor_info_2'"""
        try:
            client = self.get_influx_client()
            
            # Ensure database exists
            databases = client.get_list_database()
            if not any(db['name'] == self.influx_config['database'] for db in databases):
                client.create_database(self.influx_config['database'])
                logger.info(f"Created database: {self.influx_config['database']}")
            
            # Prepare data points for measurement 'sensor_info_2'
            # Prepare data for InfluxDB
            influx_data = []
            for index, row in df.iterrows():
                # Convert row to dict and ensure all values are properly formatted
                row_dict = row.to_dict()
                
                # Convert all values to strings to avoid parsing issues
                formatted_fields = {}
                for key, value in row_dict.items():
                    formatted_fields[key] = str(value)
                
                data_point = [{
                    "measurement": "sensor_info",
                    "tags": {},  # Empty tags
                    "fields": formatted_fields  # Use the formatted fields
                }]

                # Write the point to InfluxDB
                client.write_points(data_point)

            print("Data successfully logged into InfluxDB")
            client.close()
            
        except Exception as e:
            logger.error(f"Error saving data to InfluxDB: {e}")

# Usage
if __name__ == "__main__":
    cloud_config = {
        "host": "80.158.21.182",
        "database": "ga_database",
        "user": "root",
        "password": "b6gxcpQu5yEuntf",
        "port": 5432
    }
    
    influx_config = {
        'host': '100.92.127.116',
        'port': 8086,
        'username': 'ga_ha_influx_user',
        'password': '8666347a1afc7e596359bd01369ee00e',
        'database': 'gd_data'
    }
    
    processor = EdgeDataProcessor(cloud_config, influx_config)
    processor.process_and_save_to_influx()