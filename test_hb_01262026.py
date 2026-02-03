from influxdb import InfluxDBClient
import influxdb
import pandas as pd
from datetime import datetime
import os
import sys
#from utils import env_loader

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

# InfluxDB configurations
INFLUXDB_HOST = '100.87.234.87'
INFLUXDB_PORT = 8086
INFLUXDB_USERNAME = 'ga_influx_admin'
INFLUXDB_PASSWORD = '6ae7d195f93434099e9626b30c7c9a37'
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

def fetch_heating_rates():
    """Retrieve time_to_1_degree data for all room-radiator combinations from heating_analysis"""

    sensor_table = sensors_table(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_GD_DATABASE)

    rooms_id = set(sensor['room_id'] for sensor in sensor_table if sensor['room_id'] != 'OUT' and sensor['room_type'] == 'Active')

    # Updated query to use heating_analysis and time_to_1_degree_hours
    query_template = """
    SELECT mean("time_to_1_degree_hours") AS "time_to_1_degree", mean("average_valve_opening") AS "valve_opening"
    FROM "gd_data"."autogen"."heating_analysis"
    WHERE time > now() -30d
    AND "room"='{room}'
    AND "radiator"='{radiator_id}'
    AND "successful_session"=1
    GROUP BY "room","radiator"
    """

    query_template_last = """
    SELECT mean("time_to_1_degree_hours") AS "time_to_1_degree", mean("average_valve_opening") AS "valve_opening"
    FROM "gd_data"."autogen"."heating_analysis"
    WHERE "room"='{room}'
    AND "radiator"='{radiator_id}'
    AND "successful_session"=1
    ORDER BY time DESC
    LIMIT 5
    """
        
    results = []
    
    client_influx = InfluxDBClient(host=INFLUXDB_HOST,
                                   port=INFLUXDB_PORT,
                                   username=INFLUXDB_USERNAME,
                                   password=INFLUXDB_PASSWORD,
                                   database=INFLUXDB_GD_DATABASE)
    
    for room in rooms_id:
        print(room)

        radiator_ids = [sensor['sensor_id'] for sensor in sensor_table if sensor['room_id'] == room and sensor['sensor_type'] == "Zigbee thermostatic radiator valve"]

        for radiator_id in radiator_ids:
            query = query_template.format(
                room=room,
                radiator_id=radiator_id
            )
            result = client_influx.query(query)

            print(result)

            if not result:

                query = query_template_last.format(
                    room=room,
                    radiator_id=radiator_id
                )
                result = client_influx.query(query)

                print(result)

            if result:
                
                for point in result.get_points():
                    # Check for NaN values before adding to results
                    time_to_1_degree = point.get('time_to_1_degree')
                    valve_opening = point.get('valve_opening', 100)
                    
                    # Skip records with NaN time_to_1_degree values
                    if time_to_1_degree is None or pd.isna(time_to_1_degree) or time_to_1_degree <= 0:
                        continue
                    
                    # Convert time_to_1_degree to heating_rate equivalent (inverse relationship)
                    # Faster time_to_1_degree = better heating performance
                    # We'll use inverse: heating_rate = 1 / time_to_1_degree
                    heating_rate_equivalent = 1.0 / time_to_1_degree
                        
                    results.append({
                        'room': room,
                        'radiator_id': radiator_id,
                        'heating_rate': heating_rate_equivalent,  # Using equivalent heating rate
                        'current_valve_opening': valve_opening,
                        'time_to_1_degree_original': time_to_1_degree  # Keep original for reference
                    })

            else: 
                continue

        print(results)

    client_influx.close()
    
    # Create DataFrame and ensure no NaN values in critical columns
    df = pd.DataFrame(results)
    
    if not df.empty:
        # Remove any rows with NaN in critical columns
        df = df.dropna(subset=['heating_rate', 'current_valve_opening'])
        
        # Ensure current_valve_opening has no NaN values and convert to float
        df['current_valve_opening'] = df['current_valve_opening'].fillna(100).astype(float)
        
        # Ensure heating_rate is reasonable (not infinite from division by very small numbers)
        df = df[df['heating_rate'] < 10]  # Remove unrealistic high values
    
    return df

def calculate_balancing(df):
    """Calculate valve openings per radiator considering current valve positions"""
    if df.empty:
        return df
        
    # Create a copy to avoid SettingWithCopyWarning
    df = df.copy()
        
    if len(df) > 2:  # Only apply outlier detection if we have more than 2 rooms
        heating_rates = sorted(df['heating_rate'].tolist())
        n = len(heating_rates)
        
        # Calculate Q1 (25th percentile)
        q1_index = (n - 1) * 0.25
        if q1_index.is_integer():
            Q1 = heating_rates[int(q1_index)]
        else:
            lower_idx = int(q1_index)
            upper_idx = lower_idx + 1
            Q1 = (heating_rates[lower_idx] + heating_rates[upper_idx]) / 2
        
        # Calculate Q3 (75th percentile)
        q3_index = (n - 1) * 0.75
        if q3_index.is_integer():
            Q3 = heating_rates[int(q3_index)]
        else:
            lower_idx = int(q3_index)
            upper_idx = lower_idx + 1
            Q3 = (heating_rates[lower_idx] + heating_rates[upper_idx]) / 2
        
        IQR = Q3 - Q1
        upper_bound = Q3 + 3.0 * IQR
        
        # ONLY check for UPPER outliers (values above upper_bound)
        outlier_mask = df['heating_rate'] > upper_bound
        
        # Also check for extreme UPPER deviations from median
        median_rate = df['heating_rate'].median()
        deviation_threshold = 0.60
        # ONLY check for values significantly ABOVE median
        extreme_deviation_mask = ((df['heating_rate'] - median_rate) / median_rate) > deviation_threshold
        
        # Combine both UPPER outlier detection methods
        combined_outlier_mask = outlier_mask | extreme_deviation_mask
        outliers = df[combined_outlier_mask]
        
        if not outliers.empty:
            print(f"Removing UPPER outliers: {outliers[['room', 'heating_rate']].to_dict('records')}")
            # Keep only non-outliers (remove only high outliers)
            df = df[~combined_outlier_mask].copy()
    
    # Find room with LOWEST heating rate
    min_heating_idx = df['heating_rate'].idxmin()
    min_heating_room = df.loc[min_heating_idx, 'room']
    min_heating_rate = df.loc[min_heating_idx, 'heating_rate']
    min_current_valve = df.loc[min_heating_idx, 'current_valve_opening']

    print(f"Lowest heating rate: {min_heating_room} ({min_heating_rate:.2f}) - Current valve: {min_current_valve:.1f}%")

    # Define physical valve limits
    MIN_VALVE_PHYSICAL = 13
    MAX_VALVE_PHYSICAL = 20

    # Check if min room already at maximum valve opening
    if min_current_valve >= MAX_VALVE_PHYSICAL:
        # Already at maximum, reduce others
        print(f"{min_heating_room} already at maximum valve opening ({min_current_valve:.1f}%)")
        
        # Keep min room at its current valve opening
        df.loc[min_heating_idx, 'balanced_valve_opening_physical'] = min_current_valve
        
        # Reduce others proportionally - INVERT so highest heating rate gets lowest valve
        other_indices = df.index != min_heating_idx
        other_heating_rates = df.loc[other_indices, 'heating_rate']
        
        if len(other_heating_rates) > 0:
            # Highest heating rate = 15%, Lowest heating rate (among others) = 20%
            min_other_rate = other_heating_rates.min()
            max_other_rate = other_heating_rates.max()
            
            if max_other_rate > min_other_rate:
                # Inverted: higher rate = lower valve, lower rate = higher valve
                df.loc[other_indices, 'balanced_valve_opening_physical'] = (
                    MAX_VALVE_PHYSICAL - 
                    ((df.loc[other_indices, 'heating_rate'] - min_other_rate) / 
                     (max_other_rate - min_other_rate)) * 
                    (MAX_VALVE_PHYSICAL - MIN_VALVE_PHYSICAL)
                )
            else:
                df.loc[other_indices, 'balanced_valve_opening_physical'] = MIN_VALVE_PHYSICAL
    else:
        # Not at maximum, increase min room to maximum
        print(f"{min_heating_room} can be increased to maximum valve opening")
        df.loc[min_heating_idx, 'balanced_valve_opening_physical'] = MAX_VALVE_PHYSICAL
        
        # Scale others proportionally based on heating rate relative to min room
        other_indices = df.index != min_heating_idx
        
        # Map other rooms from MIN_VALVE to MAX_VALVE based on their heating rate vs min room heating rate
        df.loc[other_indices, 'balanced_valve_opening_physical'] = (
            MIN_VALVE_PHYSICAL + 
            (df.loc[other_indices, 'heating_rate'] / min_heating_rate) * 
            (MAX_VALVE_PHYSICAL - MIN_VALVE_PHYSICAL)
        ).clip(MIN_VALVE_PHYSICAL, MAX_VALVE_PHYSICAL - 1)

    # Round to nearest 1
    df['balanced_valve_opening_physical'] = df['balanced_valve_opening_physical'].round()

    # Calculate adjustment needed from current position (using physical values)
    df['valve_adjustment'] = (df['balanced_valve_opening_physical'] - df['current_valve_opening']).round()
    
    # Use physical values as the final balanced_valve_opening (15-20% range, 20% is maximum)
    df['balanced_valve_opening'] = df['balanced_valve_opening_physical']

    # Calculate equilibrium deviation
    df['equilibrium_deviation'] = (df['heating_rate'] - df['heating_rate'].mean()) / df['heating_rate'].mean()

    # Estimate new heating rates based on valve changes
    # Only apply change if valve adjustment is made
    # If balanced_valve_opening_physical == current_valve_opening, no change
    valve_change_ratio = df['balanced_valve_opening_physical'] / df['current_valve_opening']
    df['new_heating_rate'] = df['heating_rate'] * valve_change_ratio.clip(0.5, 1.5)

    print('main', df)

    return df

def save_results(df):
    """Save radiator-level results with both current and desired valve openings"""
    current_time = datetime.utcnow().isoformat() + "Z"
    
    # Reset index and ensure no NaN values
    df = df.reset_index()
    
    # Convert all numeric columns to float and handle NaN values
    numeric_columns = ['heating_rate', 'current_valve_opening', 'balanced_valve_opening', 
                      'valve_adjustment', 'equilibrium_deviation', 'new_heating_rate']
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Calculate inverse heating rates (time to heat up by 1 degree)
    if 'heating_rate' in df.columns:
        # Using apply with lambda to handle division by zero
        df['time_heat_up_1'] = df['heating_rate'].apply(
            lambda x: 1 / x if x != 0 and not pd.isna(x) else 0
        )
    
    if 'new_heating_rate' in df.columns:
        df['new_time_heat_up_1'] = df['new_heating_rate'].apply(
            lambda x: 1 / x if x != 0 and not pd.isna(x) else 0
        )
    
    # Also fill NaN values for original numeric columns
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    json_payload = [
        {
            "measurement": "hydraulic_balancing",
            "tags": {
                "radiator_id": row['radiator_id'],
                "room": row['room']
            },
            "time": current_time,
            "fields": {
                "heating_rate": float(row['heating_rate']),
                "current_valve_opening": float(row['current_valve_opening']),
                "balanced_valve_opening": float(row['balanced_valve_opening']),  # Physical 15-20% range
                "valve_adjustment": float(row['valve_adjustment']),
                "equilibrium_deviation": float(round(row['equilibrium_deviation'], 3)),
                "new_heating_rate": float(row['new_heating_rate']),
                "time_heat_up_1": float(row.get('time_heat_up_1', 0)),
                "new_time_heat_up_1": float(row.get('new_time_heat_up_1', 0))
            }
        } for _, row in df.iterrows()
    ]
    
    client_influx = InfluxDBClient(host=INFLUXDB_HOST,
                                   port=INFLUXDB_PORT,
                                   username=INFLUXDB_USERNAME,
                                   password=INFLUXDB_PASSWORD,
                                   database=INFLUXDB_GD_DATABASE)
    
    client_influx.write_points(json_payload)
    client_influx.close()

    return df

def hydraulic_balancing():
    try:
        df = fetch_heating_rates()
        
        if df.empty:
            raise ValueError("No time_to_1_degree data found")
            
        print(f"Processing {len(df)} radiator records...")
        balanced_df = calculate_balancing(df)
        
        # Ensure we're working with a proper DataFrame for grouping
        if not balanced_df.empty:
            print(balanced_df)
            # Don't groupby - each radiator is already a single record
            result_df = save_results(balanced_df)
            print(f"Saved balancing results for {len(result_df)} radiators")
            
            # Print summary
            print("\n=== Balancing Summary ===")
            print(result_df)
        else:
            print("No data to save after balancing calculations")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

hydraulic_balancing()