import json
import logging
import signal
import time
from kafka import KafkaProducer
import paho.mqtt.client as mqtt
from config import Config

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration as a global constant
CONFIG = Config()

class MQTTToKafkaBridge:
    """Handles the communication bridge between MQTT and Kafka."""

    def __init__(self):
        logging.info("Initializing Kafka producer...")
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=CONFIG.KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        logging.info("Kafka producer initialized.")

        logging.info("Initializing MQTT client...")
        self.mqtt_client = mqtt.Client()

        # MQTT Authentication if needed
        if CONFIG.MQTT_USERNAME and CONFIG.MQTT_PASSWORD:
            self.mqtt_client.username_pw_set(CONFIG.MQTT_USERNAME, CONFIG.MQTT_PASSWORD)

        # Set MQTT callbacks
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

        self.running = True
        logging.info("MQTT client initialized.")

    def on_connect(self, client, userdata, flags, rc):
        """Called when the MQTT client connects to the broker."""
        if rc == 0:
            logging.info("Connected to MQTT broker")
            client.subscribe(CONFIG.MQTT_TOPIC)
        else:
            logging.error(f"Failed to connect to MQTT broker, return code {rc}")

    def on_message(self, client, userdata, msg):
        """Called when an MQTT message is received."""
        logging.info(f"Received MQTT message: {msg.topic} -> {msg.payload.decode()}")
        try:
            self.send_message_to_kafka(msg.payload.decode())
        except Exception as e:
            logging.error(f"Failed to publish message to Kafka: {e}")

    def send_message_to_kafka(self, message):
        """Send message to Kafka with retry logic."""
        try:
            self.kafka_producer.send(CONFIG.KAFKA_TOPIC, message)
            logging.info(f"Sent message to Kafka topic {CONFIG.KAFKA_TOPIC}")
        except Exception as e:
            logging.error(f"Failed to send message to Kafka: {e}")

    def start(self):
        """Start the MQTT client loop."""
        logging.info("Connecting to MQTT broker...")
        self.mqtt_client.connect(CONFIG.MQTT_BROKER, CONFIG.MQTT_PORT)
        self.mqtt_client.loop_start()
        while self.running:
            time.sleep(1)

    def stop(self):
        """Stop the MQTT client loop."""
        self.running = False
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()
        self.kafka_producer.close()

def signal_handler(sig, frame):
    bridge.stop()
    logging.info("Exiting...")
    exit(0)

if __name__ == "__main__":
    bridge = MQTTToKafkaBridge()
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    bridge.start()
