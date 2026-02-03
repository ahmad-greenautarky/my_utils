import logging
import psycopg2
import requests
import pytz
import asyncio
import os
from datetime import datetime
from decimal import Decimal
from typing import List, Dict

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================== CONFIGURATION ==================
POSTGRES_CONFIG = {
    "host": "80.158.21.182",
    "database": "ga_database",
    "user": "root",
    "password": "b6gxcpQu5yEuntf",
    "port": 5432
}

TB_URL = os.getenv("THINGSBOARD_URL", "https://thingsboard.greenautarky.com")
DURATION = 7  # days

# ================== FIELD DEFINITIONS ==================
# Fields to be AVERAGED across all devices in a building
AVERAGE_FIELDS = {
    "mean_temperature": "fact_environmental_metrics",
    "mean_humidity": "fact_environmental_metrics",
    "mean_co2": "fact_environmental_metrics",
    "presence": "fact_environmental_metrics",
    "outdoor_temperature": "fact_environmental_metrics",
    "outdoor_humidity": "fact_environmental_metrics",
    "heating_rate": "fact_heating_performance",
    "initial_phase_radiator_heating_rate": "fact_heating_performance",
    "average_transfer_coefficient": "fact_environmental_metrics",
    "average_lqi": "fact_device_status",
}

# Fields to be SUMMED across all devices in a building
SUMMED_FIELDS = {
    "consumption_kWh": "fact_heating_performance",
    "expected_demand_kW": "fact_heating_performance",
    "balanced_demand_kW": "fact_heating_performance",
    "mode_auto_hours": "fact_heating_performance",
    "mode_heat_hours": "fact_heating_performance",
    "mode_off_hours": "fact_heating_performance",
    "status_idle_hours": "fact_heating_performance",
    "status_heat_hours": "fact_heating_performance",
}

# Percentage fields (should be averaged, not summed)
PERCENTAGE_FIELDS = {
    "mode_auto_percent": "fact_heating_performance",
    "mode_heat_percent": "fact_heating_performance",
    "mode_off_percent": "fact_heating_performance",
    "status_idle_percent": "fact_heating_performance",
    "status_heat_percent": "fact_heating_performance",
}

# Aggregated fields (sensor inventory, alarms, etc.)
AGGREGATE_FIELDS = {
    "offline_count": ("fact_device_status", "sum"),
    "online_count": ("fact_device_status", "sum"),
    "total_devices": ("fact_device_status", "max"),
    "count_low_lqi": ("fact_device_status", "sum"),
    "count_good_lqi": ("fact_device_status", "sum"),
    "count_low_battery": ("fact_device_status", "sum"),
    "count_good_battery": ("fact_device_status", "sum"),
    "alarms": ("fact_device_status", "sum"),
    "count_t_and_h_display": ("fact_sensor_inventory", "sum"),
    "count_thermostat": ("fact_sensor_inventory", "sum"),
    "count_ki_butler": ("fact_sensor_inventory", "sum"),
    "count_presence": ("fact_sensor_inventory", "sum"),
    "count_motion": ("fact_sensor_inventory", "sum"),
    "count_window": ("fact_sensor_inventory", "sum"),
}


class CloudToThingsBoard:
    def __init__(self, max_retries: int = 3, retry_interval: int = 300):
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        self.is_running = True
        self.conn = None

    # ==================== HELPER FUNCTIONS ====================
    def _extract_scalar_value(self, value):
        """
        Extract a scalar value from potentially nested structures.
        Handles lists, arrays, tuples, and other complex types.
        
        Args:
            value: The value to extract (could be list, dict, scalar, etc.)
        
        Returns:
            Scalar value or None if extraction fails
        """
        if value is None:
            return None
        
        # If it's a list or tuple, try to get the first element
        if isinstance(value, (list, tuple)):
            if len(value) == 0:
                return None
            return self._extract_scalar_value(value[0])  # Recursively handle nested structures
        
        # If it's a dict, try to extract a numeric value
        if isinstance(value, dict):
            for key in ['value', 'val', 'amount', 'count', 'data']:
                if key in value and value[key] is not None:
                    return self._extract_scalar_value(value[key])
            return None
        
        # Return as-is for scalar types (int, float, str, Decimal, etc.)
        return value

    def _safe_float_convert(self, value, device_id: str, field: str) -> float:
        """
        Safely convert a value to float with detailed logging on failure.
        
        Args:
            value: Value to convert
            device_id: Device ID for logging context
            field: Field name for logging context
        
        Returns:
            Float value or None if conversion fails
        """
        if value is None:
            return None
        
        try:
            return float(value)
        except (ValueError, TypeError) as e:
            logger.warning(
                f"Could not convert value to float for device {device_id}, "
                f"field '{field}': {value!r} (type: {type(value).__name__}) - {e}"
            )
            return None

    # ==================== DATABASE CONNECTION ====================
    def connect_db(self):
        self.conn = psycopg2.connect(**POSTGRES_CONFIG)
        return self.conn

    def close_db(self):
        if self.conn:
            self.conn.close()

    # ==================== FETCH BUILDINGS WITH TB TOKENS ====================
    def fetch_buildings_with_tokens(self) -> List[Dict]:
        """
        Fetch all buildings from dim_tb_building with their tb_device_token_hash.
        The token hash is retrieved directly from the database.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT building_id, building_name, tb_device_token FROM dim_tb_building ORDER BY building_name;"
            )
            results = cursor.fetchall()
            cursor.close()

            logger.info(f"Fetched buildings from database: {results}")

            buildings = []
            for row in results:
                building_id = str(row[0])
                building_name = str(row[1])
                tb_token_hash = row[2]

                # Convert memoryview to bytes, then to string
                if isinstance(tb_token_hash, memoryview):
                    tb_token_hash = tb_token_hash.tobytes().decode('utf-8')
                elif isinstance(tb_token_hash, bytes):
                    tb_token_hash = tb_token_hash.decode('utf-8')
                
                print(f"Building: {building_name}, Token: {tb_token_hash}")
                
                if tb_token_hash:
                    buildings.append({
                        "building_id": building_id,
                        "building_name": building_name,
                        "tb_device_token_hash": tb_token_hash
                    })
                    logger.info(f"Loaded token for building: {building_name}")
                else:
                    logger.warning(
                        f"No token found for building '{building_name}' ({building_id})."
                    )

            logger.info(f"Fetched {len(buildings)} buildings with tokens")
            return buildings

        except Exception as e:
            logger.error(f"Error fetching buildings: {e}")
            return []

    # ==================== FETCH DEVICE-TO-APARTMENT MAPPING ====================
    def fetch_device_to_apartment_mapping(self, building_id: str) -> Dict[str, str]:
        """
        Fetch mapping of device_id to apartment_id for a specific building.
        Returns: {device_id: apartment_id}
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT device_id, apartment_id 
                FROM dim_apartment
                WHERE building_id = %s AND device_id IS NOT NULL;
                """,
                (building_id,)
            )
            results = cursor.fetchall()
            cursor.close()

            device_mapping = {str(row[0]): str(row[1]) for row in results}
            logger.info(f"Fetched {len(device_mapping)} device-to-apartment mappings for building {building_id}")
            return device_mapping

        except Exception as e:
            logger.error(f"Error fetching device-to-apartment mapping for building {building_id}: {e}")
            return {}

    # ==================== FETCH DATA ====================
    def fetch_table(self, table: str, fields: List[str]) -> List[Dict]:
        """
        Fetch data from a table for all devices in the last DURATION days.
        Includes device_id and timestamp for grouping/filtering.
        """
        try:
            cursor = self.conn.cursor()
            base_fields = ["device_id", "timestamp"]
            fields_sql = ", ".join(base_fields + fields) if fields else "*"

            query = f"""
                SELECT {fields_sql}
                FROM {table}
                WHERE timestamp > NOW() - INTERVAL '{DURATION} days'
                ORDER BY timestamp DESC
            """
            print(query)
            cursor.execute(query)

            rows = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            cursor.close()

            return [dict(zip(colnames, row)) for row in rows]

        except Exception as e:
            logger.error(f"Error fetching {table}: {str(e)}")
            return []

    def fetch_table_by_device(self, table: str, fields: List[str], device_id: str) -> List[Dict]:
        """
        Fetch data from a table filtered by a specific device_id in the last DURATION days.
        This is the main query method - filters data per device.
        """
        try:
            cursor = self.conn.cursor()
            base_fields = ["device_id", "timestamp"]
            fields_sql = ", ".join(base_fields + fields) if fields else "*"

            query = f"""
                SELECT {fields_sql}
                FROM {table}
                WHERE device_id = %s AND timestamp > NOW() - INTERVAL '{DURATION} days'
                ORDER BY timestamp DESC
            """
            cursor.execute(query, (device_id,))

            rows = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            cursor.close()

            result = [dict(zip(colnames, row)) for row in rows]
            logger.debug(f"Fetched {len(result)} rows from {table} for device {device_id}")
            return result

        except Exception as e:
            logger.error(f"Error fetching {table} for device {device_id}: {str(e)}")
            return []

    # ==================== DATA AGGREGATION & AVERAGING ====================
    def aggregate_device_data(self, device_id: str, apartment_id: str) -> Dict:
        """
        Fetch and aggregate data for a single device.
        
        Process:
        1. Fetch all data from fact tables filtered by device_id
        2. Apply aggregation rules (average latest values)
        3. Return single data point for this device
        
        FIXED: Handles list/array values from database with proper type extraction
        """
        aggregated_data = {
            "device_id": device_id,
            "apartment_id": apartment_id,
            "timestamp": datetime.now(pytz.utc),
        }

        device_id_str = str(device_id)

        # -------- AVERAGE FIELDS --------
        for field, table in AVERAGE_FIELDS.items():
            data = self.fetch_table_by_device(table, [field], device_id_str)
            
            if data:
                logger.debug(f"Processing AVERAGE field '{field}' from table '{table}'")
                values = []
                
                for d in data:
                    raw_value = d.get(field)
                    if raw_value is not None:
                        scalar_value = self._extract_scalar_value(raw_value)
                        if scalar_value is not None:
                            float_value = self._safe_float_convert(scalar_value, device_id_str, field)
                            if float_value is not None:
                                values.append(float_value)
                
                if values:
                    aggregated_data[field] = sum(values) / len(values)
                    logger.debug(
                        f"Device {device_id}: Averaged {field}: {aggregated_data[field]} "
                        f"(from {len(values)} values)"
                    )

        # -------- SUMMED FIELDS --------
        for field, table in SUMMED_FIELDS.items():
            data = self.fetch_table_by_device(table, [field], device_id_str)

            if data:
                logger.debug(f"Processing SUMMED field '{field}' from table '{table}'")
                values = []
                
                for d in data:
                    raw_value = d.get(field)
                    if raw_value is not None:
                        scalar_value = self._extract_scalar_value(raw_value)
                        if scalar_value is not None:
                            float_value = self._safe_float_convert(scalar_value, device_id_str, field)
                            if float_value is not None:
                                values.append(float_value)
                
                if values:
                    aggregated_data[field] = sum(values)
                    logger.debug(f"Device {device_id}: Summed {field}: {aggregated_data[field]} "
                                f"(from {len(values)} values)")

        # -------- PERCENTAGE FIELDS (AVERAGED) --------
        for field, table in PERCENTAGE_FIELDS.items():
            data = self.fetch_table_by_device(table, [field], device_id_str)

            if data:
                logger.debug(f"Processing PERCENTAGE field '{field}' from table '{table}'")
                values = []
                
                for d in data:
                    raw_value = d.get(field)
                    if raw_value is not None:
                        scalar_value = self._extract_scalar_value(raw_value)
                        if scalar_value is not None:
                            float_value = self._safe_float_convert(scalar_value, device_id_str, field)
                            if float_value is not None:
                                values.append(float_value)
                
                if values:
                    aggregated_data[field] = sum(values) / len(values)
                    logger.debug(
                        f"Device {device_id}: Averaged percentage {field}: {aggregated_data[field]} "
                        f"(from {len(values)} values)"
                    )

        # -------- AGGREGATED FIELDS (COUNTS, ALARMS) --------
        for field, (table, agg_type) in AGGREGATE_FIELDS.items():
            data = self.fetch_table_by_device(table, [field], device_id_str)

            if data:
                logger.debug(f"Processing AGGREGATED field '{field}' ({agg_type}) from table '{table}'")
                values = []
                
                for d in data:
                    raw_value = d.get(field)
                    if raw_value is not None:
                        scalar_value = self._extract_scalar_value(raw_value)
                        if scalar_value is not None:
                            float_value = self._safe_float_convert(scalar_value, device_id_str, field)
                            if float_value is not None:
                                values.append(float_value)
                
                if values:
                    if agg_type == "sum":
                        aggregated_data[field] = sum(values)
                    elif agg_type == "max":
                        aggregated_data[field] = max(values)
                    elif agg_type == "min":
                        aggregated_data[field] = min(values)
                    elif agg_type == "avg":
                        aggregated_data[field] = sum(values) / len(values)
                    
                    logger.debug(
                        f"Device {device_id}: Aggregated {field} ({agg_type}): {aggregated_data[field]} "
                        f"(from {len(values)} values)"
                    )

        return aggregated_data

    
    # ==================== AGGREGATE ALL DEVICES IN BUILDING ====================
    def aggregate_building_data(self, building_id: str, device_mapping: Dict[str, str]) -> Dict:
        """
        Aggregate data across all devices in a building.
        
        Process:
        1. For each device in the building, aggregate its device data
        2. Combine all device data using the aggregation rules:
           - AVERAGE_FIELDS: average across all devices
           - SUMMED_FIELDS: sum across all devices
           - PERCENTAGE_FIELDS: average across all devices
           - AGGREGATE_FIELDS: apply aggregation function (sum/max/min/avg)
        3. Return single aggregated payload for the entire building
        """
        logger.info(f"Starting aggregation for {len(device_mapping)} devices in building {building_id}")
        
        # Collect data for all devices
        all_device_data = []
        for device_id, apartment_id in device_mapping.items():
            device_data = self.aggregate_device_data(device_id, apartment_id)
            print(f"Device data for {device_id}: {device_data}")
            if device_data and len(device_data) > 3:  # Has more than just metadata
                all_device_data.append(device_data)
                logger.debug(f"Collected data for device {device_id}")
            else:
                logger.warning(f"No data for device {device_id}")

        if not all_device_data:
            logger.error(f"No valid device data collected for building {building_id}")
            return {}

        logger.info(f"Successfully collected data from {len(all_device_data)} devices")

        # Build aggregated building data
        building_aggregated = {
            "building_id": building_id,
            "device_count": len(all_device_data),
            "timestamp": datetime.now(pytz.utc),
        }

        # -------- AVERAGE FIELDS ACROSS ALL DEVICES --------
        for field in AVERAGE_FIELDS.keys():
            values = []
            for device_data in all_device_data:
                if field in device_data and device_data[field] is not None:
                    values.append(device_data[field])
            
            if values:
                avg_value = sum(values) / len(values)
                building_aggregated[field] = avg_value
                logger.debug(f"Building aggregate - Averaged {field}: {avg_value} (from {len(values)} devices)")

        # -------- SUMMED FIELDS ACROSS ALL DEVICES --------
        for field in SUMMED_FIELDS.keys():
            values = []
            for device_data in all_device_data:
                if field in device_data and device_data[field] is not None:
                    values.append(device_data[field])
            
            if values:
                sum_value = sum(values)
                building_aggregated[field] = sum_value
                logger.debug(f"Building aggregate - Summed {field}: {sum_value} (from {len(values)} devices)")

        # -------- PERCENTAGE FIELDS (AVERAGED ACROSS DEVICES) --------
        for field in PERCENTAGE_FIELDS.keys():
            values = []
            for device_data in all_device_data:
                if field in device_data and device_data[field] is not None:
                    values.append(device_data[field])
            
            if values:
                avg_value = sum(values) / len(values)
                building_aggregated[field] = avg_value
                logger.debug(f"Building aggregate - Averaged percentage {field}: {avg_value} (from {len(values)} devices)")

        # -------- AGGREGATED FIELDS (COUNTS, ALARMS) --------
        for field, (table, agg_type) in AGGREGATE_FIELDS.items():
            values = []
            for device_data in all_device_data:
                if field in device_data and device_data[field] is not None:
                    values.append(device_data[field])
            
            if values:
                if agg_type == "sum":
                    agg_value = sum(values)
                elif agg_type == "max":
                    agg_value = max(values)
                elif agg_type == "min":
                    agg_value = min(values)
                elif agg_type == "avg":
                    agg_value = sum(values) / len(values)
                
                building_aggregated[field] = agg_value
                logger.debug(f"Building aggregate - {field} ({agg_type}): {agg_value} (from {len(values)} devices)")

        logger.info(f"Building {building_id} aggregation complete with {len(building_aggregated)} fields")
        return building_aggregated

    # ==================== SEND TO THINGSBOARD ====================
    def send_to_thingsboard(self, data: Dict, tb_token_hash, building_name: str) -> bool:
        """
        Send aggregated building data to ThingsBoard.
        
        Note: tb_token_hash is the BYTEA hash from the database.
        If this is the actual token to use, convert from bytes to string.
        If this is just a hash and you need the original token, you'll need to
        retrieve it from environment variables or a secrets manager.
        """
        try:
            print('token', tb_token_hash)
            # Convert BYTEA (bytes) to string if needed
            if isinstance(tb_token_hash, bytes):
                tb_token = tb_token_hash.decode('utf-8')
            else:
                tb_token = str(tb_token_hash)
            
            # Parse and convert timestamp
            try:
                if isinstance(data["timestamp"], str):
                    dt = datetime.fromisoformat(data["timestamp"])
                else:
                    dt = data["timestamp"]
            except Exception:
                dt = datetime.now(pytz.utc)

            # Ensure timezone awareness
            if dt.tzinfo is None:
                dt = pytz.utc.localize(dt)
            dt = dt.astimezone(pytz.timezone("Europe/Berlin"))

            ts = int(dt.timestamp() * 1000)

            # Build payload with all aggregated values (excluding metadata)
            payload = {
                "ts": ts,
                "values": {
                    field: float(value) if isinstance(value, Decimal) else value
                    for field, value in data.items()
                    if field not in ["building_id", "timestamp", "device_count"] 
                    and value is not None
                }
            }

            # ==================== PRINT DATA BEFORE SENDING ====================
            print("\n" + "="*80)
            print(f"SENDING BUILDING DATA TO THINGSBOARD")
            print("="*80)
            print(f"Building: {building_name}")
            print(f"Device Count: {data.get('device_count')}")
            print(f"Token (first 20 chars): {tb_token[:20]}...")
            print(f"URL: {TB_URL}/api/v1/{tb_token[:20]}...{tb_token[-10:]}/telemetry")
            print(f"Timestamp: {dt} (ts: {ts})")
            print(f"\nFields to send ({len(payload['values'])} total):")
            for field, value in sorted(payload["values"].items()):
                print(f"  - {field}: {value}")
            print(f"\nFull Payload:")
            import json
            print(json.dumps(payload, indent=2, default=str))
            print("="*80 + "\n")
            # =====================================================================

            print("⏳ Sending aggregated building data to ThingsBoard...", tb_token)

            response = requests.post(
                f"{TB_URL}/api/v1/{tb_token}/telemetry",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )

            if response.status_code == 200:
                logger.info(f"Successfully sent aggregated building data for: {building_name}")
                print(f"✅ SUCCESS: Aggregated data sent to ThingsBoard for {building_name}")
                return True
            else:
                logger.error(
                    f"Failed sending for building {building_name}: "
                    f"{response.status_code} {response.text}"
                )
                print(f"❌ FAILED: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error sending data to ThingsBoard: {str(e)}")
            print(f"❌ ERROR: {str(e)}")
            return False

    # ==================== MAIN COLLECTION LOGIC ====================
    async def collect_and_send(self):
        """
        Main loop:
        1. Fetch all buildings with TB tokens
        2. For each building:
           a. Fetch device-to-apartment mapping
           b. AGGREGATE all devices in the building using defined rules
           c. Send single aggregated payload to ThingsBoard
        
        KEY CHANGE: Instead of sending per device, we now:
        - Collect all device data first
        - Apply building-wide aggregation rules
        - Send ONE payload per building
        """
        conn = self.connect_db()

        buildings = self.fetch_buildings_with_tokens()
        if not buildings:
            logger.error("No buildings found")
            return

        for building in buildings:
            building_id = building["building_id"]
            building_name = building["building_name"]
            tb_token_hash = building["tb_device_token_hash"]

            logger.info(f"Processing building: {building_name} ({building_id})")

            # Get device-to-apartment mapping for this building
            device_mapping = self.fetch_device_to_apartment_mapping(building_id)
            if not device_mapping:
                logger.warning(f"No devices found for building {building_id}")
                continue

            logger.info(f"  Found {len(device_mapping)} devices in building {building_name}")
            print(f"  Device mapping: {device_mapping}")

            # ==================== AGGREGATE ALL DEVICES IN BUILDING ====================
            building_data = self.aggregate_building_data(building_id, device_mapping)

            if not building_data or len(building_data) <= 2:  # Only building_id, device_count, timestamp
                logger.warning(f"No aggregated data for building {building_id}")
                continue

            # ==================== PRINT AGGREGATED BUILDING DATA ====================
            print(f"\n{'─'*80}")
            print(f"AGGREGATED BUILDING DATA - {building_name}")
            print(f"{'─'*80}")
            import json
            print(json.dumps({k: v for k, v in building_data.items() 
                            if k not in ['timestamp']}, indent=2, default=str))
            print(f"{'─'*80}\n")
            # ==============================================================

            # ==================== SEND ONCE PER BUILDING ====================
            if self.send_to_thingsboard(building_data, tb_token_hash, building_name):
                logger.info(f"Successfully sent aggregated data for building {building_id}")
            else:
                logger.warning(f"Failed to send aggregated data for building {building_id}")

        conn.close()

    # ==================== SYNCHRONOUS RUN ==================
    def run_once_sync(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self.collect_and_send())
        finally:
            loop.close()
            self.close_db()


# ================== MAIN ==================
if __name__ == "__main__":
    sender = CloudToThingsBoard(max_retries=3, retry_interval=300)
    sender.run_once_sync()