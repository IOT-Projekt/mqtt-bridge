import json
import logging
import signal
from typing import Any, Dict, Optional
from kafka import KafkaProducer, KafkaConsumer
import paho.mqtt.client as mqtt
from config import Config

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration as a global constant
CONFIG: Config = Config()

class MQTTToKafkaBridge:
    """Handles the communication bridge between MQTT and Kafka."""

    def __init__(self) -> None:
        self.kafka_producer: KafkaProducer = self.setup_kafka_producer()
        self.kafka_consumer: KafkaConsumer = self.setup_kafka_consumer()
        self.mqtt_client: mqtt.Client = self.setup_mqtt_client()
        self.running: bool = True

    def setup_kafka_producer(self) -> KafkaProducer:
        """Initialize Kafka producer."""
        logging.info("Initializing Kafka producer...")
        producer = KafkaProducer(
            bootstrap_servers=CONFIG.KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        logging.info("Kafka producer initialized.")
        return producer

    def setup_kafka_consumer(self) -> KafkaConsumer:
        """Initialize Kafka consumer."""
        logging.info("Initializing Kafka consumer...")
        consumer = KafkaConsumer(
            *CONFIG.KAFKA_TOPIC_MAPPING.values(),
            bootstrap_servers=CONFIG.KAFKA_BROKER,
            value_deserializer=self.json_deserializer,
        )
        logging.info("Kafka consumer initialized.")
        return consumer

    def json_deserializer(self, message: bytes) -> Optional[Dict[str, Any]]:
        """Deserialize JSON message."""
        try:
            return json.loads(message.decode("utf-8"))
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON message: {e}")
            return None

    def setup_mqtt_client(self) -> mqtt.Client:
        """Initialize MQTT client."""
        logging.info("Initializing MQTT client...")
        client = mqtt.Client()

        # if username and password are set, use them for authentication
        if CONFIG.MQTT_USERNAME and CONFIG.MQTT_PASSWORD:
            client.username_pw_set(CONFIG.MQTT_USERNAME, CONFIG.MQTT_PASSWORD)

        # Set own method callbacks
        client.on_connect = self.on_mqtt_connect
        client.on_message = self.on_mqtt_message

        logging.info("MQTT client initialized.")
        return client

    def on_mqtt_connect(self, client: mqtt.Client, userdata, flags, rc: int) -> None:
        """Called when the MQTT client connects to the broker."""
        if rc == 0:
            logging.info("Connected to MQTT broker")
            for topic in CONFIG.KAFKA_TOPIC_MAPPING.keys():
                client.subscribe(topic)
                logging.info(f"Subscribed to MQTT topic: {topic}")
        else:
            logging.error(f"Failed to connect to MQTT broker, return code {rc}")

    def on_mqtt_message(self, client: mqtt.Client, userdata, msg: mqtt.MQTTMessage) -> None:
        """Called when an MQTT message is received."""
        
        msg_content: str = msg.payload.decode()
        logging.info(f"Received MQTT message: {msg.topic} -> {msg_content}")
        
        # Check if the message is from MQTT source to avoid infinite loop
        try:
            msg_json: Dict[str, Any] = json.loads(msg_content)
            if msg_json.get("source") != "mqtt":
                logging.info("Skipping message from non MQTT source")
                return
        except json.JSONDecodeError:
            logging.error("Failed to decode MQTT message as JSON")
        
        # Check if the topic has a mapping to a kafka topic and send the message
        try:
            kafka_topic = CONFIG.KAFKA_TOPIC_MAPPING.get(msg.topic)
            if kafka_topic:
                self.send_message_to_kafka(kafka_topic, msg_content)
            else:
                logging.error(f"No Kafka topic mapping found for MQTT topic {msg.topic}")
        except Exception as e:
            logging.error(f"Failed to publish message to Kafka: {e}")
            
    def send_message_to_kafka(self, kafka_topic: str, message: str) -> None:
        """Send the mqtt message to the specified Kafka topic."""
        try:
            self.kafka_producer.send(kafka_topic, {'message': message})
            logging.info(f"Sent message to Kafka topic {kafka_topic}")
        except Exception as e:
            logging.error(f"Failed to send message to Kafka: {e}")
            
    def send_message_to_mqtt(self, mqtt_topic: str, message: str) -> None:
        """Send the Kafka message to the specified MQTT topic."""
        try:
            self.mqtt_client.publish(mqtt_topic, message)
            logging.info(f"Sent message to MQTT topic {mqtt_topic}")
        except Exception as e:
            logging.error(f"Failed to send message to MQTT: {e}")
            
    def start(self) -> None:
        """Start the MQTT client loop and Kafka consumer loop."""
        # start mqtt client
        logging.info("Connecting to MQTT broker...")
        self.mqtt_client.connect(CONFIG.MQTT_BROKER, CONFIG.MQTT_PORT)
        logging.info("Starting MQTT client loop...")
        self.mqtt_client.loop_start()
        
        # start kafka consumer
        logging.info("Starting Kafka consumer loop...")
        while self.running:
            try:
                for message in self.kafka_consumer:
                    logging.info(f"Received Kafka message: {message.topic} -> {message.value}") 
                    message_json = json.loads(message.value.get("message"))
                    
                    # Check if the message is from a Kafka source to avoid infinite loop
                    if message_json.get("source") != "kafka":
                        logging.info("Skipping message from non Kafka source")
                        continue
                    
                    mqtt_topic = self.get_mqtt_topic_for_kafka_topic(message.topic)
                    if mqtt_topic is not None:
                        self.send_message_to_mqtt(mqtt_topic, json.dumps(message_json))
                    else:
                        logging.error(f"No MQTT topic mapping found for Kafka topic {message.topic}")
            except Exception as e:
                logging.error(f"Error processing Kafka message: {e}")

    def stop(self) -> None:
        """Stop the MQTT client loop and Kafka consumer loop."""
        self.running = False
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()
        self.kafka_producer.close()
        self.kafka_consumer.close()

    def get_mqtt_topic_for_kafka_topic(self, kafka_topic: str) -> str:
        """Get the corresponding MQTT topic for a given Kafka topic."""
        try:
            for mqtt_topic, kt in CONFIG.KAFKA_TOPIC_MAPPING.items():
                if kt == kafka_topic:
                    return mqtt_topic
            return None
        except Exception as e:
            logging.error(f"Failed to get MQTT topic for Kafka topic: {e}") 
            return None

def signal_handler(sig, frame):
    bridge.stop()
    logging.info("Exiting...")
    exit(0)

if __name__ == "__main__":
    bridge = MQTTToKafkaBridge()
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    bridge.start()
