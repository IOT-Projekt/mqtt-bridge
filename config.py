import os
import json
import logging
from typing import Dict

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
        self.MQTT_BROKER: str = os.getenv("MQTT_BROKER", "mqtt.example.com")
        self.MQTT_PORT: int = int(os.getenv("MQTT_PORT", 1883))
        self.KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "kafka.example.com:9092")
        
        # Optional MQTT authentication
        self.MQTT_USERNAME: str = os.getenv("MQTT_USERNAME", None)
        self.MQTT_PASSWORD: str = os.getenv("MQTT_PASSWORD", None)

        # MQTT topics mapping to Kafka topics        
        kafka_topic_mapping: str = os.getenv("KAFKA_TOPIC_MAPPING", '{}')
        self.KAFKA_TOPIC_MAPPING: Dict[str, str] = json.loads(kafka_topic_mapping)
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
