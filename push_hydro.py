from influxdb import InfluxDBClient
import pandas as pd
from datetime import datetime
import os
import sys

INFLUXDB_HOST = '100.87.234.87'
INFLUXDB_PORT = 8086
INFLUXDB_USERNAME = 'ga_influx_admin'
INFLUXDB_PASSWORD = '6ae7d195f93434099e9626b30c7c9a37'
INFLUXDB_DATABASE = 'ga_homeassistant_db'
INFLUXDB_GD_DATABASE = "gd_data"
DURATION = '30d'

def push_valve_openings():
    """Push raw valve opening values to InfluxDB"""
    current_time = datetime.utcnow().isoformat() + "Z"
    
    # Define the valve opening values you want to set
    valve_openings = {
        "kidsroom": 11.0,
        "bedroom": 11.0,
        "kitchen": 12.0,
        "livingroom": 15.0
    }
    
    json_payload = [
        {
            "measurement": "hydraulic_balancing",
            "time": current_time,
            "fields": {
                f"balanced_valve_opening_{room}": float(opening)
            }
        } for room, opening in valve_openings.items()
    ]
    
    try:
        client_influx = InfluxDBClient(
            host=INFLUXDB_HOST,
            port=INFLUXDB_PORT,
            username=INFLUXDB_USERNAME,
            password=INFLUXDB_PASSWORD,
            database=INFLUXDB_GD_DATABASE
        )
        
        client_influx.write_points(json_payload)
        client_influx.close()
        
        print(f"Successfully pushed valve openings to InfluxDB at {current_time}")
        print("Values pushed:")
        for room, opening in valve_openings.items():
            print(f"  - balanced_valve_opening_{room}: {opening}")
            
    except Exception as e:
        print(f"Error pushing to InfluxDB: {str(e)}")
        import traceback
        traceback.print_exc()


# You can also create a more flexible version that accepts custom values:
def push_custom_valve_openings(valve_openings_dict):
    """Push custom valve opening values to InfluxDB
    
    Args:
        valve_openings_dict (dict): Dictionary with room names as keys and 
                                   valve opening values as values
                                   Example: {"kidsroom": 100.0, "bedroom": 100.0}
    """
    current_time = datetime.utcnow().isoformat() + "Z"
    
    json_payload = [
        {
            "measurement": "hydraulic_balancing",
            "time": current_time,
            "fields": {
                f"balanced_valve_opening_{room}": float(opening)
            }
        } for room, opening in valve_openings_dict.items()
    ]
    
    try:
        client_influx = InfluxDBClient(
            host=INFLUXDB_HOST,
            port=INFLUXDB_PORT,
            username=INFLUXDB_USERNAME,
            password=INFLUXDB_PASSWORD,
            database=INFLUXDB_GD_DATABASE
        )
        
        client_influx.write_points(json_payload)
        client_influx.close()
        
        print(f"Successfully pushed custom valve openings to InfluxDB at {current_time}")
        print("Values pushed:")
        for room, opening in valve_openings_dict.items():
            print(f"  - balanced_valve_opening_{room}: {opening}")
            
    except Exception as e:
        print(f"Error pushing to InfluxDB: {str(e)}")
        import traceback
        traceback.print_exc()

push_valve_openings()