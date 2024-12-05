import json
import logging
from kafka import KafkaProducer
import signal
import paho.mqtt.client as mqtt
from config import Config

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration as a global constant
CONFIG = Config()

class MQTTToKafkaBridge:
    """Handles the communication bridge between MQTT and Kafka."""

    def __init__(self):
        # Set up Kafka producer and MQTT client 
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=CONFIG.KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.mqtt_client = mqtt.Client()

        # if MQTT username and password are provided, set them for authentication
        if CONFIG.MQTT_USERNAME and CONFIG.MQTT_PASSWORD:
            self.mqtt_client.username_pw_set(CONFIG.MQTT_USERNAME, CONFIG.MQTT_PASSWORD)

        # Set MQTT callbacks 
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

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

    def _handle_signal(self, sig, frame):
        """Handle termination signals."""
        logging.info(f"Received termination signal: {sig}")
        self.stop()

    def start(self):
        """Start the MQTT client loop."""
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        
        self.mqtt_client.connect(CONFIG.MQTT_BROKER, CONFIG.MQTT_PORT)
        try:
            self.mqtt_client.loop_start()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """Stop the MQTT client loop."""
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()
        self.kafka_producer.close()

if __name__ == "__main__":
    bridge = MQTTToKafkaBridge()
    bridge.start()
