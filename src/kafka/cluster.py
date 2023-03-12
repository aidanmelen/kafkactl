from confluent_kafka import KafkaException
from confluent_kafka.admin import ConfigResource
from .kafka_resource import KafkaResource
from .consumer_group import ConsumerGroup

class Cluster(KafkaResource):
    def __init__(self, admin_client):
        """
        The Kafka Cluster wrapper class.

        Args:
            admin_client (kafka.admin.client.AsyncAdminClient): The Kafka AdminClient instance.
        """
        super().__init__(admin_client=admin_client)
    
    def get(self, timeout=10):
        """
        Get information about the Kafka Cluster.

        Args:
            timeout (int, optional): The time (in seconds) to wait for the operation to complete before timing out.

        Returns:
            dict: The metadata for the Kafka Cluster.
        
        Raises:
            KafkaError: If there is an error during the process.
        """
        metadata = self.admin_client.list_topics(timeout=timeout)

        brokers = []
        for broker_id, broker_metadata in metadata.brokers.items():
            brokers.append({
                "name": broker_id,
                "type": "controller" if broker_id == metadata.controller_id else "worker",
                "endpoint": f"{broker_metadata.host}:{broker_metadata.port}"
            })

        return brokers
    
    def get_default_configs(self, timeout=10):
        """
        Get the Kafka cluster default configuration.

        Returns:
            dict: The Kafka cluster default configuration.
        
        Raises:
            KafkaError: If there is an error during the process.
        """
        metadata = self.admin_client.list_topics(timeout=timeout)
        broker_id = str(list(metadata.brokers.keys())[0])
        resources = [ConfigResource("broker", broker_id)]
        future = self.admin_client.describe_configs(resources)

        results = {}
        for res, f in future.items():
            cluster_default_config = f.result()
            results = {config.name: config.value for config in cluster_default_config.values()}
        
        return results
    
    def create(self):
        raise NotImplemented

    def describe(self, timeout=10):
        """
        Describe one or many Kafka Brokers.

        Args:
            timeout (int, optional): The time (in seconds) to wait for the operation to complete before timing out.

        Returns:
            dict: The detailed information for the Kafka Brokers.
        
        Raises:
            KafkaError: If there is an error during the describe process.
        """
        metadata = self.admin_client.list_topics(timeout=timeout)
        
        results = {}
        topics = []
        partitions = []
        replicas = []
        
        for topic_name, topic in metadata.topics.items():
            topics.append(topic)

            for partition in topic.partitions.values():
                partitions.append(partition)

                for broker in partition.replicas:
                    replicas.append(replicas)
        
        
        group = ConsumerGroup(self.admin_client)
        groups = group.get(timeout=timeout)
        
        results["brokers"] = len(metadata.brokers.values())
        results["topics"] = len(topics)
        results["partitions"] = len(partitions)
        results["replicas"] = len(replicas)
        results["consumer_groups"] = len(groups)

        return results
        
    def alter(self):
        raise NotImplemented

    def delete(self):
        raise NotImplemented