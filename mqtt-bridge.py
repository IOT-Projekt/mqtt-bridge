import json
import logging
import signal
import time
from kafka import KafkaProducer
import paho.mqtt.client as mqtt
from config import Config

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MQTTToKafkaBridge:
    """Handles the communication bridge between MQTT and Kafka."""

    def __init__(self, config: Config):
        self.config = config
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=self.config.KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.mqtt_client = mqtt.Client()

        # MQTT Authentication if needed
        if self.config.MQTT_USERNAME and self.config.MQTT_PASSWORD:
            self.mqtt_client.username_pw_set(self.config.MQTT_USERNAME, self.config.MQTT_PASSWORD)

        # Set MQTT callbacks
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

        self.running = True

    def on_connect(self, client, userdata, flags, rc):
        """Called when the MQTT client connects to the broker."""
        if rc == 0:
            logging.info("Connected to MQTT broker")
            client.subscribe(self.config.MQTT_TOPIC)
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
        retries = 3
        for attempt in range(retries):
            try:
                self.kafka_producer.send(self.config.KAFKA_TOPIC, value=message)
                self.kafka_producer.flush()
                logging.info("Message forwarded to Kafka")
                break
            except Exception as e:
                logging.error(f"Error sending message to Kafka (Attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(5)  # Retry after a short delay
                else:
                    logging.error("Max retries reached, message not sent to Kafka")

    def start(self):
        """Start the MQTT to Kafka bridge."""
        try:
            signal.signal(signal.SIGTERM, self._handle_signal)
            signal.signal(signal.SIGINT, self._handle_signal)

            self.mqtt_client.connect(self.config.MQTT_BROKER, self.config.MQTT_PORT)
            logging.info(f"Connecting to MQTT broker at {self.config.MQTT_BROKER}:{self.config.MQTT_PORT}")
            self.mqtt_client.loop_start()

            while self.running:
                time.sleep(1)

        except Exception as e:
            logging.error(f"Unexpected error: {e}")
        finally:
            self.stop()

    def stop(self):
        """Gracefully shutdown MQTT and Kafka connections."""
        logging.info("Stopping MQTT to Kafka bridge...")
        self.running = False
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()
        self.kafka_producer.close()
        logging.info("Bridge stopped.")

    def _handle_signal(self, sig, frame):
        """Handle termination signals."""
        logging.info(f"Received termination signal: {sig}")
        self.stop()


def main():
    """Main entry point to initialize and run the bridge."""
    try:
        config = Config()
        config.validate()
        bridge = MQTTToKafkaBridge(config)
        logging.info("Starting MQTT to Kafka bridge...")
        bridge.start()
    except ValueError as ve:
        logging.error(f"Configuration error: {ve}")
        exit(1)
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
        exit(1)


if __name__ == "__main__":
    main()
