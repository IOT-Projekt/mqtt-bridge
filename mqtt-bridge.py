import os
import json
import logging
from kafka import KafkaProducer
import paho.mqtt.client as mqtt

# Set up logging
logging.basicConfig(level=logging.INFO)

# External MQTT and Kafka broker settings
MQTT_BROKER = os.getenv('MQTT_BROKER', 'mqtt.example.com')  # External MQTT broker
MQTT_PORT = int(os.getenv('MQTT_PORT', 1883))
MQTT_TOPIC = os.getenv('MQTT_TOPIC', 'mqtt/topic')
MQTT_USERNAME = os.getenv('MQTT_USERNAME', None)
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD', None)

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka.example.com:9092')  # External Kafka broker
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'kafka_topic')

# Initialize Kafka producer
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# MQTT callback functions
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC)
    elif rc == 5:
        logging.error("Authentication failed: Not authorized")
    else:
        logging.error(f"Failed to connect to MQTT broker, return code {rc}")

def on_message(client, userdata, msg):
    logging.info(f"Received MQTT message: {msg.topic} -> {msg.payload.decode()}")
    try:
        # Forward message to Kafka
        kafka_producer.send(KAFKA_TOPIC, value=msg.payload.decode())
        kafka_producer.flush()
        logging.info("Message forwarded to Kafka")
    except Exception as e:
        logging.error(f"Failed to publish message to Kafka: {e}")

# MQTT client setup
mqtt_client = mqtt.Client()

# MQTT authentication if needed
if MQTT_USERNAME and MQTT_PASSWORD:
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

try:
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
except Exception as e:
    logging.error(f"Error connecting to MQTT broker: {e}")
    exit(1)

# Start MQTT loop to handle incoming messages
if __name__ == "__main__":
    try:
        logging.info("Starting MQTT to Kafka bridge...")
        mqtt_client.loop_forever()
    except KeyboardInterrupt:
        logging.info("Stopping bridge...")
    finally:
        mqtt_client.disconnect()
        kafka_producer.close()
