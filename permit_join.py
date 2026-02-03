import paho.mqtt.client as mqtt
import json
import time

# MQTT configuration
MQTT_BROKER = '100.72.46.17'  # e.g., "localhost" or "192.168.1.100"
MQTT_PORT = 1883
MQTT_USERNAME = 'ga_zigbee2mqtt'  # Optional
MQTT_PASSWORD = 'ga_zigbee2mqtt' # Optional
Z2M_TOPIC = "zigbee2mqtt/bridge/request/permit_join"

def enable_permit_join(MQTT_BROKER, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD, timeout=254):
    """
    Enable permit join in Zigbee2MQTT
    
    Args:
        timeout (int): Time in seconds until permit join automatically disables (max 254)
    Returns:
        dict: Status and message
    """
    client = mqtt.Client()
    
    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    
    payload = {
        "value": True,
        "time": timeout
    }
    
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.publish(Z2M_TOPIC, json.dumps(payload))
        time.sleep(1)
        
        return {
            "status": "on",
            "duration": timeout,
            "message": f"Permit join enabled for {timeout} seconds"
        }
        
    except Exception as e:
        return {
            "status": "error",
            "duration": 0,
            "message": f"Error enabling permit join: {str(e)}"
        }
    finally:
        client.disconnect()

def disable_permit_join(MQTT_BROKER, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD):
    """
    Disable permit join immediately
    Returns:
        dict: Status and message
    """
    client = mqtt.Client()
    
    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    
    payload = {
        "value": False
    }
    
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.publish(Z2M_TOPIC, json.dumps(payload))
        time.sleep(1)
        
        return {
            "status": "off",
            "duration": 0,
            "message": "Permit join disabled"
        }
        
    except Exception as e:
        return {
            "status": "error",
            "duration": 0,
            "message": f"Error disabling permit join: {str(e)}"
        }
    finally:
        client.disconnect()

permit_result = enable_permit_join(
                    MQTT_BROKER, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD
                )    

print(permit_result)