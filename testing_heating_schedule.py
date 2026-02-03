from datetime import datetime
from influxdb import InfluxDBClient
import pytz
import influxdb

INFLUXDB_HOST = '100.87.234.87'
INFLUXDB_PORT = 8086
INFLUXDB_USERNAME = 'ga_influx_admin'
INFLUXDB_PASSWORD = '6ae7d195f93434099e9626b30c7c9a37'
INFLUXDB_DATABASE = 'ga_homeassistant_db'
INFLUXDB_GD_DATABASE = "gd_data"

def push_heating_schedule_to_influx(
    client_influx,
    start_time,
    end_time,
    room="bedroom",
    database="gd_data",
    measurement="heating_schedule"
):
    """
    Push heating schedule data to InfluxDB v1.
    
    Args:
        client_influx: InfluxDBClient instance
        start_time: datetime object or ISO string for schedule start
        end_time: datetime object or ISO string for schedule end
        room: room tag (default: "bedroom")
        database: InfluxDB database name (default: "gd_data")
        measurement: measurement name (default: "heating_schedule")
    """
    
    berlin_tz = pytz.timezone('Europe/Berlin')
    
    # Convert inputs to timezone-aware datetime objects
    if isinstance(start_time, str):
        start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
    if isinstance(end_time, str):
        end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
    
    # Ensure times are timezone-aware, convert to UTC
    if start_time.tzinfo is None:
        start_time = berlin_tz.localize(start_time)
    if end_time.tzinfo is None:
        end_time = berlin_tz.localize(end_time)
    
    start_time_utc = start_time.astimezone(pytz.UTC)
    end_time_utc = end_time.astimezone(pytz.UTC)
    
    # Create JSON payload for InfluxDB v1
    json_body = [
        {
            "measurement": measurement,
            "tags": {
                "room_id": room
            },
            "time": datetime.now(pytz.UTC).isoformat(),
            "fields": {
                "start_time": start_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "end_time": end_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
            }
        }
    ]
    
    # Write to InfluxDB
    client_influx.write_points(json_body, database=database, time_precision='u')
    
    print(f"✓ Pushed heating schedule for {room}")
    print(f"  Start: {start_time_utc.isoformat()}")
    print(f"  End: {end_time_utc.isoformat()}")


# Example usage:
if __name__ == "__main__":
    # Initialize client for InfluxDB v1
    client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST,
                        port=INFLUXDB_PORT,
                        username=INFLUXDB_USERNAME,
                        password=INFLUXDB_PASSWORD,
                        database=INFLUXDB_DATABASE)
    
    # Push a heating schedule
    push_heating_schedule_to_influx(
        client_influx=client_influx,
        start_time="2025-12-12T11:00:00Z",
        end_time="2025-12-12T11:00:00Z",
        room="badezimmer"
    )
    pass