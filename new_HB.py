"""Hydraulic Balancing System with Monitoring Capabilities"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import influxdb

INFLUXDB_HOST = '100.84.82.25'
MQTT_HOST = '100.84.82.25'
MQTT_PORT = 1883  # Change if your MQTT broker uses a different port
MQTT_USERNAME = 'ga_zigbee2mqtt'  # Add your MQTT username if needed
MQTT_PASSWORD = 'ga_zigbee2mqtt'
INFLUXDB_PORT = 8086
INFLUXDB_USERNAME = 'ga_influx_admin'
INFLUXDB_PASSWORD = 'root'
INFLUXDB_DATABASE = 'ga_homeassistant_db'
INFLUXDB_GD_DATABASE = "gd_data"

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

sensor_table = sensors_table(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, 
                      INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)

def get_heating_rates(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, 
                      INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE, 
                      days_back=30):
    """Retrieve heating rates for all room-radiator combinations"""
    
    sensor_table = sensors_table(INFLUXDB_HOST, INFLUXDB_PORT, 
                                 INFLUXDB_USERNAME, INFLUXDB_PASSWORD, 
                                 INFLUXDB_GD_DATABASE)
    
    rooms_id = set(sensor['room_id'] for sensor in sensor_table 
                   if sensor['room_id'] != 'OUT' and sensor['room_type'] == 'Active')
    

    # Enhanced query to get both radiator and room heating rates
    query_template = '''
    SELECT mean("heating_rate") AS "heating_rate"
    FROM "gd_data"."autogen"."preprocessed_heating_rate"
    WHERE time > now() - {days}d
    AND "{room}"='{radiator_id}'
    '''
    
    results = []
    
    client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST,
                                   port=INFLUXDB_PORT,
                                   username=INFLUXDB_USERNAME,
                                   password=INFLUXDB_PASSWORD,
                                   database=INFLUXDB_GD_DATABASE)
    
    for room in rooms_id:
        print(f"Processing room: {room}")
        
        # Get radiator sensors
        radiator_ids = [sensor['sensor_id'] for sensor in sensor_table 
                       if sensor['room_id'] == room 
                       and sensor['sensor_type'] == "Zigbee thermostatic radiator valve"]
        
        # Get room temperature sensor if available
        room_temp_sensors = [sensor['sensor_id'] for sensor in sensor_table 
                            if sensor['room_id'] == room 
                            and 'temperature' in sensor.get('sensor_type', '').lower()]
        
        for radiator_id in radiator_ids:


            # Get radiator heating rate
            query = query_template.format(
                days=days_back,
                room=room,
                radiator_id=radiator_id
            )
            print('test', room, radiator_id)
            print(query)
            result = client_influx.query(query)
            print(result)
            
            # Get room heating rate if room sensor exists
            room_heating_rate = None
            if room_temp_sensors:
                room_query = query_template.format(
                    days=days_back,
                    room=room,
                    radiator_id=room_temp_sensors[0]
                )
                room_result = client_influx.query(room_query)
                room_points = list(room_result.get_points())
                if room_points:
                    room_heating_rate = room_points[0]['heating_rate']
            
            for point in result.get_points():
                results.append({
                    'room': room,
                    'radiator_id': radiator_id,
                    'radiator_heating_rate': point['heating_rate'],
                    'room_heating_rate': room_heating_rate
                })
    
    client_influx.close()
    return pd.DataFrame(results)


def calculate_balancing(df, method='proportional', target_percentile=50):
    """
    Calculate valve openings per radiator with multiple methods
    
    Parameters:
    -----------
    df : DataFrame with heating rates
    method : str
        'proportional' - Inverse proportional to heating rate
        'target_based' - Aim for target heating rate
        'room_normalized' - Consider room heating rate as well
    target_percentile : int (default 50)
        Which percentile to target (50 = median)
    """
    if df.empty:
        return df
    
    # Calculate target heating rate based on percentile
    target_rate = df['radiator_heating_rate'].quantile(target_percentile / 100)
    
    if method == 'proportional':
        # Original method: radiator with lowest rate gets 100%
        min_rate = df['radiator_heating_rate'].min()
        df['valve_opening'] = (min_rate / df['radiator_heating_rate']) * 100
        
    elif method == 'target_based':
        # Target-based: aim for median heating rate across all radiators
        df['valve_opening'] = (target_rate / df['radiator_heating_rate']) * 100
        
    elif method == 'room_normalized':
        # Consider room heating rate efficiency
        # Rooms with high radiator rate but low room rate need restriction
        df['heating_efficiency'] = df.apply(
            lambda row: row['room_heating_rate'] / row['radiator_heating_rate'] 
            if row['room_heating_rate'] is not None and row['radiator_heating_rate'] > 0
            else 1.0, 
            axis=1
        )
        
        # Adjust valve based on both radiator rate and efficiency
        avg_efficiency = df['heating_efficiency'].mean()
        df['valve_opening'] = (target_rate / df['radiator_heating_rate']) * \
                              (df['heating_efficiency'] / avg_efficiency) * 100
    
    # Apply practical limits (40-100%) and round to 5%
    df['valve_opening'] = (df['valve_opening']
                          .clip(40, 100)
                          .apply(lambda x: 5 * round(x/5))
                          .astype(float))
    
    # Calculate deviations
    df['equilibrium_deviation'] = (
        (df['radiator_heating_rate'] - target_rate) / target_rate
    )
    
    # Estimate new heating rate after balancing
    # Assumption: heating rate scales with square root of valve opening
    # (more realistic than linear due to flow characteristics)
    df['estimated_new_rate'] = df['radiator_heating_rate'] * \
                               np.sqrt(df['valve_opening'] / 100.0)
    
    # Calculate expected balance improvement
    current_std = df['radiator_heating_rate'].std()
    new_std = df['estimated_new_rate'].std()
    df['balance_improvement'] = (current_std - new_std) / current_std * 100
    
    return df


def create_baseline(df, INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME,
                   INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE):
    """
    Create baseline reference for future monitoring
    Stores current balanced state as reference
    """
    current_time = datetime.utcnow().isoformat() + "Z"
    
    json_payload = [
        {
            "measurement": "hydraulic_balancing_baseline",
            "tags": {
                "room": row['room'],
                "radiator_id": row['radiator_id']
            },
            "time": current_time,
            "fields": {
                "reference_radiator_rate": row['radiator_heating_rate'],
                "reference_valve_opening": row['valve_opening'],
                "reference_room_rate": row['room_heating_rate'] 
                    if row['room_heating_rate'] is not None else 0.0
            }
        } for _, row in df.iterrows()
    ]
    
    client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST,
                                   port=INFLUXDB_PORT,
                                   username=INFLUXDB_USERNAME,
                                   password=INFLUXDB_PASSWORD,
                                   database=INFLUXDB_GD_DATABASE)
    
    client_influx.write_points(json_payload)
    client_influx.close()
    
    print(f"Baseline created with {len(df)} radiators")


def monitor_balancing(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME,
                     INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE,
                     days_back=1, deviation_threshold=0.20):
    """
    Monitor current heating session against baseline
    
    Parameters:
    -----------
    days_back : int
        Number of days to look back for current data
    deviation_threshold : float
        Alert if deviation exceeds this (0.20 = 20%)
    
    Returns:
    --------
    DataFrame with monitoring results and alerts
    """
    
    # Get current heating rates
    current_df = get_heating_rates(INFLUXDB_HOST, INFLUXDB_PORT,
                                   INFLUXDB_USERNAME, INFLUXDB_PASSWORD,
                                   INFLUXDB_GD_DATABASE,
                                   days_back=days_back)
    
    # Get baseline reference
    client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST,
                                   port=INFLUXDB_PORT,
                                   username=INFLUXDB_USERNAME,
                                   password=INFLUXDB_PASSWORD,
                                   database=INFLUXDB_GD_DATABASE)
    
    baseline_query = '''
    SELECT last("reference_radiator_rate") AS "ref_rate",
           last("reference_valve_opening") AS "ref_valve",
           last("reference_room_rate") AS "ref_room_rate"
    FROM "gd_data"."autogen"."hydraulic_balancing_baseline"
    GROUP BY "room", "radiator_id"
    '''
    
    baseline_result = client_influx.query(baseline_query)
    client_influx.close()
    
    # Parse baseline
    baseline_data = []
    for (room, radiator), points in baseline_result.items():
        for point in points:
            baseline_data.append({
                'room': room,
                'radiator_id': radiator,
                'ref_radiator_rate': point['ref_rate'],
                'ref_valve': point['ref_valve'],
                'ref_room_rate': point['ref_room_rate']
            })
    
    baseline_df = pd.DataFrame(baseline_data)
    
    # Merge current with baseline
    monitoring_df = pd.merge(
        current_df, 
        baseline_df,
        on=['room', 'radiator_id'],
        how='inner'
    )
    
    # Calculate deviations
    monitoring_df['radiator_rate_deviation'] = (
        (monitoring_df['radiator_heating_rate'] - monitoring_df['ref_radiator_rate']) / 
        monitoring_df['ref_radiator_rate']
    )
    
    monitoring_df['room_rate_deviation'] = monitoring_df.apply(
        lambda row: (row['room_heating_rate'] - row['ref_room_rate']) / row['ref_room_rate']
        if row['room_heating_rate'] is not None and row['ref_room_rate'] > 0
        else None,
        axis=1
    )
    
    # Flag alerts
    monitoring_df['alert'] = monitoring_df['radiator_rate_deviation'].abs() > deviation_threshold
    monitoring_df['alert_type'] = monitoring_df.apply(
        lambda row: 'FAST_HEATING' if row['radiator_rate_deviation'] > deviation_threshold
        else ('SLOW_HEATING' if row['radiator_rate_deviation'] < -deviation_threshold
              else 'OK'),
        axis=1
    )
    
    # Calculate system-wide balance metric
    current_cv = monitoring_df['radiator_heating_rate'].std() / \
                 monitoring_df['radiator_heating_rate'].mean()
    baseline_cv = monitoring_df['ref_radiator_rate'].std() / \
                  monitoring_df['ref_radiator_rate'].mean()
    
    print(f"\n=== Monitoring Summary ===")
    print(f"Radiators monitored: {len(monitoring_df)}")
    print(f"Current balance (CV): {current_cv:.3f}")
    print(f"Baseline balance (CV): {baseline_cv:.3f}")
    print(f"Alerts: {monitoring_df['alert'].sum()}")
    
    if monitoring_df['alert'].any():
        print(f"\n⚠️ ALERTS:")
        alerts = monitoring_df[monitoring_df['alert']][
            ['room', 'radiator_id', 'alert_type', 'radiator_rate_deviation']
        ]
        for _, alert in alerts.iterrows():
            print(f"  {alert['room']}/{alert['radiator_id']}: "
                  f"{alert['alert_type']} ({alert['radiator_rate_deviation']:.1%} deviation)")
    
    return monitoring_df


def save_results(df, INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME,
                INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE):
    """Save radiator-level balancing results"""
    current_time = datetime.utcnow().isoformat() + "Z"
    
    json_payload = [
        {
            "measurement": "hydraulic_balancing",
            "tags": {
                "room": row['room'],
                "radiator_id": row['radiator_id']
            },
            "time": current_time,
            "fields": {
                "radiator_heating_rate": row['radiator_heating_rate'],
                "room_heating_rate": row['room_heating_rate'] 
                    if row['room_heating_rate'] is not None else 0.0,
                "valve_opening": row['valve_opening'],
                "equilibrium_deviation": round(row['equilibrium_deviation'], 3),
                "estimated_new_rate": row['estimated_new_rate'],
                "balance_improvement": row['balance_improvement'].iloc[0] 
                    if 'balance_improvement' in row else 0.0
            }
        } for _, row in df.iterrows()
    ]
    
    client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST,
                                   port=INFLUXDB_PORT,
                                   username=INFLUXDB_USERNAME,
                                   password=INFLUXDB_PASSWORD,
                                   database=INFLUXDB_GD_DATABASE)
    
    client_influx.write_points(json_payload)
    client_influx.close()
    
    print(f"Saved {len(df)} balancing calculations to InfluxDB")


# Example usage workflow
def main_balancing_workflow():
    """
    Complete workflow: calculate balancing, create baseline, monitor
    """
    
    # Step 1: Get heating rates
    df = get_heating_rates(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, 
                      INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)
    
    # Step 2: Calculate valve openings (choose method)
    df_balanced = calculate_balancing(df, method='target_based', target_percentile=50)
    
    # Step 3: Save results
    #save_results(df_balanced, ...)
    
    # Step 4: Create baseline for monitoring
    create_baseline(df_balanced, INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, 
                      INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)
    
    print("\n=== Balancing Results ===")
    print(df_balanced[['room', 'radiator_id', 'radiator_heating_rate', 
                       'valve_opening', 'estimated_new_rate']])
    
    # Step 5 (Later): Monitor against baseline
    # monitoring_df = monitor_balancing(..., days_back=1, deviation_threshold=0.20)

main_balancing_workflow()