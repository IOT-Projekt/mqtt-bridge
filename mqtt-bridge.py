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
        self.kafka_producer = self.setup_kafka_connection()
        self.mqtt_client = self.setup_mqtt_client()
        self.running = True

    def setup_kafka_connection(self):
        """Initialize Kafka producer."""
        logging.info("Initializing Kafka producer...")
        producer = KafkaProducer(
            bootstrap_servers=CONFIG.KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        logging.info("Kafka producer initialized.")
        return producer

    def setup_mqtt_client(self):
        """Initialize MQTT client."""
        logging.info("Initializing MQTT client...")
        client = mqtt.Client()

        # MQTT Authentication if needed
        if CONFIG.MQTT_USERNAME and CONFIG.MQTT_PASSWORD:
            client.username_pw_set(CONFIG.MQTT_USERNAME, CONFIG.MQTT_PASSWORD)

        # Set MQTT callbacks
        client.on_connect = self.on_connect
        client.on_message = self.on_message

        logging.info("MQTT client initialized.")
        return client

    def on_connect(self, client, userdata, flags, rc):
        """Called when the MQTT client connects to the broker."""
        if rc == 0:
            logging.info("Connected to MQTT broker")
            for topic in CONFIG.MQTT_TOPICS:
                client.subscribe(topic)
                logging.info(f"Subscribed to MQTT topic: {topic}")
        else:
            logging.error(f"Failed to connect to MQTT broker, return code {rc}")

    def on_message(self, client, userdata, msg):
        """Called when an MQTT message is received."""
        logging.info(f"Received MQTT message: {msg.topic} -> {msg.payload.decode()}")
        
        # Check if the topic has a mappint to a kafka topic and send the message
        try:
            kafka_topic = CONFIG.KAFKA_TOPIC_MAPPING.get(msg.topic)
            if kafka_topic:
                self.send_message_to_kafka(kafka_topic, msg.payload.decode())
            else:
                logging.error(f"No Kafka topic mapping found for MQTT topic {msg.topic}")
        except Exception as e:
            logging.error(f"Failed to publish message to Kafka: {e}")

    def send_message_to_kafka(self, kafka_topic, message):
        """Send the mqtt message to the specified Kafka topic."""
        try:
            self.kafka_producer.send(kafka_topic, {'message': message})
            logging.info(f"Sent message to Kafka topic {kafka_topic}")
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
