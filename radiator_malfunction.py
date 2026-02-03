import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
import influxdb

# InfluxDB configurations
HOST = '100.123.187.31' #'100.115.73.108'  # Change to localhost
INFLUXDB_HOST = '100.123.187.31' #'100.115.73.108' #'d730d661-ga-influxdbv1'
INFLUXDB_PORT = 8086
INFLUXDB_USER = 'ga_influx_admin'
INFLUXDB_PASSWORD = 'root'
INFLUXDB_DATABASE = 'ga_homeassistant_db'
INFLUXDB_GD_DATABASE = "gd_data"

client_influx = influxdb.InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USER, password=INFLUXDB_PASSWORD, database=INFLUXDB_DATABASE)


query_radiator_data = f'SELECT ("local_temperature_calibration") AS "mean_local_temperature_calibration",  ("local_temperature") AS "mean_local_temperature", ("running_state") AS "mode" FROM "gd_data"."autogen"."radiator_data" WHERE time < now() - 1d AND "ROOM4"=\'0x38398ffffe034019\''
radiator_data = client_influx.query(query_radiator_data)
radiator_data = pd.DataFrame(list(radiator_data.get_points()))
print(radiator_data)


df = pd.DataFrame(radiator_data)

df['temperature'] = df['mean_local_temperature'] - df['mean_local_temperature_calibration']

# Simulate malfunction (temperature rising during idle mode)
df.loc[70:90, 'temperature'] = df.loc[70:90, 'temperature'].to_numpy() + np.linspace(0, 3, 21)

# Temperature change between readings
df['temp_diff'] = df['temperature'].diff()

# Malfunction detection logic
df['anomaly'] = (
    (df['temp_diff'] > 0) & (df['mode'] == 'idle') |   # Heating during idle
    (df['temp_diff'] <= 0) & (df['mode'] == 'heat') # No heating when in heating mode
)

# Plot the temperature and highlight anomalies
plt.plot(df['time'].to_numpy(), df['temperature'].to_numpy(), label='Radiator Temperature (°C)')
plt.scatter(df['time'][df['anomaly']].to_numpy(), df['temperature'][df['anomaly']].to_numpy(), color='red', label='TRV Malfunction')
plt.xticks(rotation=45)
plt.legend()
plt.grid(True)
plt.show()