import os
import json
import logging

class Config:
    """Encapsulates configuration for MQTT and Kafka using Singleton pattern."""
    
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Initialize configuration values."""
        # MQTT and Kafka configuration
        self.MQTT_BROKER = os.getenv("MQTT_BROKER", "mqtt.example.com")
        self.MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
        self.KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka.example.com:9092")
        
        # Optional MQTT authentication
        self.MQTT_USERNAME = os.getenv("MQTT_USERNAME", None)
        self.MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", None)

        # MQTT topics mapping to Kafka topics        
        kafka_topic_mapping = os.getenv("KAFKA_TOPIC_MAPPING", '{}')
        self.KAFKA_TOPIC_MAPPING = json.loads(kafka_topic_mapping)
        logging.info(f"KAFKA_TOPIC_MAPPING: {kafka_topic_mapping}")
        
        logging.info(f"KAFKA_TOPIC_MAPPING: {self.KAFKA_TOPIC_MAPPING}")
        logging.info(f"MQTT_BROKER: {self.MQTT_BROKER}")
        logging.info(f"KAFKA_BROKER: {self.KAFKA_BROKER}")
        

    def validate(self):
        """Validate required configuration fields."""
        if not self.MQTT_BROKER:
            raise ValueError("MQTT_BROKER must be set.")
        if not self.MQTT_TOPICS:
            raise ValueError("MQTT_TOPICS must be set.")
        if not self.KAFKA_TOPIC_MAPPING:
            raise ValueError("KAFKA_TOPIC_MAPPING must be set.")
