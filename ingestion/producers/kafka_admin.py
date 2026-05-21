from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"

TOPICS = [
    NewTopic(name="orders-raw", num_partitions=6, replication_factor=1),
    NewTopic(name="payments-raw", num_partitions=6, replication_factor=1),
    NewTopic(name="inventory-raw", num_partitions=3, replication_factor=1),
    NewTopic(name="clicks-raw", num_partitions=6, replication_factor=1),
    NewTopic(name="dlq-events", num_partitions=1, replication_factor=1),
]


def create_topics():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    for topic in TOPICS:
        try:
            admin.create_topics([topic])
            logger.info(
                f"Created topic: {topic.name} " f"({topic.num_partitions} partitions)"
            )
        except TopicAlreadyExistsError:
            logger.info(f"Topic already exists: {topic.name}")
    admin.close()


def list_topics():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    topics = admin.list_topics()
    logger.info(f"All topics: {sorted(topics)}")
    admin.close()


if __name__ == "__main__":
    create_topics()
    list_topics()
