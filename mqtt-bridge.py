import json
import logging
import signal
from kafka import KafkaProducer, KafkaConsumer
import paho.mqtt.client as mqtt
from config import Config

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration as a global constant
CONFIG = Config()

class MQTTToKafkaBridge:
    """Handles the communication bridge between MQTT and Kafka."""

    def __init__(self):
        self.kafka_producer = self.setup_kafka_producer()
        self.kafka_consumer = self.setup_kafka_consumer()
        self.mqtt_client = self.setup_mqtt_client()
        self.running = True

    def setup_kafka_producer(self):
        """Initialize Kafka producer."""
        logging.info("Initializing Kafka producer...")
        producer = KafkaProducer(
            bootstrap_servers=CONFIG.KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        logging.info("Kafka producer initialized.")
        return producer

    def setup_kafka_consumer(self):
        """Initialize Kafka consumer."""
        logging.info("Initializing Kafka consumer...")
        consumer = KafkaConsumer(
            *CONFIG.KAFKA_TOPIC_MAPPING.values(),
            bootstrap_servers=CONFIG.KAFKA_BROKER,
            value_deserializer=self.json_deserializer,
        )
        logging.info("Kafka consumer initialized.")
        return consumer

    def json_deserializer(self, message):
        """Deserialize JSON message."""
        try:
            return json.loads(message.decode("utf-8"))
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON message: {e}")
            return None

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
        
        msg_content = msg.payload.decode()
        
        logging.info(f"Received MQTT message: {msg.topic} -> {msg_content}")
        
        # Check if the message is from MQTT source to avoid infinite loop
        try:
            msg_json = json.loads(msg_content)
            logging.info(f"Message JSON: {msg_json}") #TODO DELETE LATER
            if msg_json.get("source") != "mqtt":
                return
        except json.JSONDecodeError:
            logging.error("Failed to decode MQTT message as JSON")

        # Remove the source field to avoid infinite loop
        msg_json["source"] = ""
        msg_content = json.dumps(msg_json)
        logging.info(f"Message content: {msg_content}") #TODO DELETE LATER
        
        # Check if the topic has a mapping to a kafka topic and send the message
        try:
            kafka_topic = CONFIG.KAFKA_TOPIC_MAPPING.get(msg.topic)
            if kafka_topic:
                self.send_message_to_kafka(kafka_topic, msg_content)
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
            
    def send_message_to_mqtt(self, mqtt_topic, message):
        """Send the Kafka message to the specified MQTT topic."""
        logging.info(f"Sending message to MQTT topic: {mqtt_topic}") #TODO: remove
        logging.info(f"Message: {message}") #TODO: remove
        logging.info(f"Message type: {type(message)}") #TODO: remove
        try:
            self.mqtt_client.publish(mqtt_topic, message)
            logging.info(f"Sent message to MQTT topic {mqtt_topic}")
        except Exception as e:
            logging.error(f"Failed to send message to MQTT: {e}")
            
    def start(self):
        """Start the MQTT client loop and Kafka consumer loop."""
        logging.info("Connecting to MQTT broker...")
        self.mqtt_client.connect(CONFIG.MQTT_BROKER, CONFIG.MQTT_PORT)
        logging.info("Starting MQTT client loop...")
        self.mqtt_client.loop_start()
        
        logging.info("Starting Kafka consumer loop...")
        while self.running:
            try:
                for message in self.kafka_consumer:
                    logging.info(f"Received Kafka message: {message.topic} -> {message.value}") #TODO: remove
                    logging.info(f"Source of message: {message.value.get('source')}") #TODO: remove
                    # Check if the message is from Kafka source to avoid infinite loop
                    if message.value.get("source") != "kafka":
                        continue
                    
                    logging.info(f"Will forward message to MQTT") #TODO: remove
                    mqtt_topic = self.get_mqtt_topic_for_kafka_topic(message.topic)
                    if mqtt_topic is not None:
                        self.send_message_to_mqtt(mqtt_topic, json.dumps(message.value))
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

    def get_mqtt_topic_for_kafka_topic(self, kafka_topic) -> str:
        """Get the corresponding MQTT topic for a given Kafka topic."""
        logging.info(f"Getting MQTT topic for Kafka topic: {kafka_topic}") #TODO: remove
        try:
            for mqtt_topic, kt in CONFIG.KAFKA_TOPIC_MAPPING.items():
                if kt == kafka_topic:
                    logging.info(f"Found MQTT topic for Kafka topic: {mqtt_topic}") #TODO: remove
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
