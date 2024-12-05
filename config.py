import os

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
        
        # MQTT configuration
        self.MQTT_BROKER = os.getenv("MQTT_BROKER", "mqtt.example.com")
        self.MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
        self.MQTT_TOPIC = os.getenv("MQTT_TOPIC", "mqtt/topic")
        self.MQTT_USERNAME = os.getenv("MQTT_USERNAME", None)
        self.MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", None)

        # Kafka configuration
        self.KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka.example.com:9092")
        self.KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "kafka_topic")
        
        # Validate configuration
        self.validate()

    def validate(self):
        """Validate required configuration fields."""
        if not self.MQTT_BROKER or not self.KAFKA_BROKER:
            raise ValueError("MQTT_BROKER and KAFKA_BROKER must be set.")
        if not self.MQTT_TOPIC or not self.KAFKA_TOPIC:
            raise ValueError("MQTT_TOPIC and KAFKA_TOPIC must be set.")

