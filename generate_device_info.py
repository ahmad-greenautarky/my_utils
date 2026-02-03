import influxdb
from datetime import datetime



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

def get_device_identifiers():
    """Fetch MAC address and Home Assistant Instance ID"""
    
    # Get MAC Address
    mac_address = None
    try:
        with open('/sys/class/net/end0/address', 'r') as f:
            mac_address = f.read().strip()
    except FileNotFoundError:
        print("Warning: Could not read MAC address from end0")
        # Fallback: try other interfaces
        for interface in ['eth0', 'ens18', 'enp0s3']:
            try:
                with open(f'/sys/class/net/{interface}/address', 'r') as f:
                    mac_address = f.read().strip()
                    break
            except FileNotFoundError:
                continue
    
    # Get Home Assistant Instance ID
    ha_instance_id = None
    try:
        with open('/config/.storage/core.uuid', 'r') as f:
            uuid_data = json.load(f)
            ha_instance_id = uuid_data.get('data', {}).get('uuid')
    except FileNotFoundError:
        print("Warning: Could not read HA instance ID file")
    except json.JSONDecodeError:
        print("Warning: Could not parse HA instance ID file")
    
    return {
        'mac_address': mac_address,
        'ha_instance_id': ha_instance_id
    }

def init_influxdb_client(host, port, username, password, database):
    """Initialize InfluxDB client"""
    try:
        client = connect_or_create_database(
            host=host,
            port=port,
            user=username,
            password=password,
            dbname=database
        )
        # Test connection
        client.ping()
        print(f"Connected to InfluxDB at {host}:{port}")
        return client
    except Exception as e:
        print(f"Error connecting to InfluxDB: {e}")
        return None

def save_device_info_to_influxdb(client, identifiers):
    """Save device info to InfluxDB"""
    
    # Build InfluxDB point with device info as fields
    json_body = [
        {
            "measurement": "device_info",
            "tags": {},  # No tags needed, using fields for the identifiers
            "time": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            "fields": {
                "ha_instance_id": identifiers['ha_instance_id'],
                "mac_address": identifiers['mac_address']
            }
        }
    ]
    
    try:
        client.write_points(json_body)
        print(f"Device info written to InfluxDB")
        print(f"  - HA Instance ID: {identifiers['ha_instance_id']}")
        print(f"  - MAC Address: {identifiers['mac_address']}")
        return True
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")
        return False

# Test it
def generate_device_info():
    print("Testing device identifier retrieval...")
    identifiers = get_device_identifiers()
    
    print("\n=== Device Identifiers ===")
    print(f"MAC Address: {identifiers['mac_address']}")
    print(f"HA Instance ID: {identifiers['ha_instance_id']}")
    
    # InfluxDB Configuration
    INFLUXDB_CONFIG = {
        'host': 'kibu-00000001.tail6e296.ts.net',  # or your InfluxDB host IP
        'port': 8086,
        'username': 'ga_influx_admin',
        'password': 'b2b5e29f2b3445d1bd5690e69e421324',
        'database': 'gd_data'
    }
    
    # Initialize InfluxDB client
    client = init_influxdb_client(**INFLUXDB_CONFIG)
    
    if client and identifiers['ha_instance_id'] and identifiers['mac_address']:
        print("\n=== Writing to InfluxDB ===")
        success = save_device_info_to_influxdb(client, identifiers)
        
        if success:
            print("\n=== Query Example ===")
            # Query the data back
            query = "SELECT * FROM device_info ORDER BY time DESC LIMIT 5"
            result = client.query(query)
            points = list(result.get_points())
            print(f"Recent device info entries:")
            for point in points:
                print(f"  - Time: {point['time']}, UUID: {point['ha_instance_id']}, MAC: {point['mac_address']}")
    else:
        print("\nFailed to initialize InfluxDB client or retrieve identifiers")

generate_device_info()