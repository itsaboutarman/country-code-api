import json
import redis
from kafka import KafkaProducer
from app.core.config import settings

# Redis Client Setup
redis_client = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    decode_responses=True  
)

# Kafka Producer Setup
def get_kafka_producer():
    """
    Initializes and returns a Kafka producer.
    We use a function so we can handle connection errors gracefully.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        return producer
    except Exception as e:
        print(f"Warning: Could not connect to Kafka. Error: {e}")
        return None

kafka_producer = get_kafka_producer()