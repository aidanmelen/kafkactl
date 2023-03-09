from confluent_kafka import KafkaException
from confluent_kafka.admin import ConfigResource
from .kafka_resource import KafkaResource
from .consumer_group import ConsumerGroup

class Broker(KafkaResource):
    def __init__(self, admin_client):
        """
        The Kafka Broker class.

        Args:
            admin_client (kafka.admin.client.AsyncAdminClient): The Kafka AdminClient instance.
        """
        super().__init__(admin_client=admin_client)
    
    def list(self, timeout=10):
        metadata = self.admin_client.list_topics(timeout=timeout)

        brokers = []
        for b in iter(metadata.brokers.values()):
            brokers.append({
                "name": f"broker.{b.id}",
                "type": "controller" if b.id ==  metadata.controller_id else "worker",
            })

        return brokers
    
    def create(self):
        raise NotImplemented

    def describe(self, timeout=10):
        """
        Describe one or many Kafka Brokers.

        Args:
            timeout (int, optional): The time (in seconds) to wait for the operation to complete before timing out.

        Returns:
            dict: The broker metadata, including info and/or config. An error dictionary if both info or config are False.
        
        Raises:
            KafkaError: If there is an error during the describe process.
        """
        brokers_metadata = self.admin_client.list_topics(timeout=timeout)
        
        brokers = {}
        topics = []
        partitions = []
        replicas = []
        
        for topic_name, topic in brokers_metadata.topics.items():
            topics.append(topic)

            for partition in topic.partitions.values():
                partitions.append(partition)

                for broker in partition.replicas:
                    replicas.append(replicas)
        
        group = ConsumerGroup(self.admin_client)
        groups = group.list(timeout=timeout)
        
        brokers["topics"] = len(topics)
        brokers["partitions"] = len(partitions)
        brokers["replicas"] = len(replicas)
        brokers["consumer_groups"] = len(groups)

        return brokers

    def get_cluster_defaults(self, timeout=10):
        """
        Get configuration for the Kafka Cluster.

        Returns:
            dict: The configuration for the Kafka Brokers.
        
        Raises:
            KafkaError: If there is an error during the describe process.
        """
        brokers_metadata = self.admin_client.list_topics(timeout=timeout)
        broker_id = str(list(brokers_metadata.brokers.keys())[0])
        resources = [ConfigResource("broker", broker_id)]
        future = self.admin_client.describe_configs(resources)

        brokers = {}
        for res, f in future.items():
            configs = f.result()

            brokers = {}
            brokers = {config.name: config.value for config in configs.values()}
        
        return brokers
        
    def alter(self):
        raise NotImplemented

    def delete(self):
        raise NotImplemented