from confluent_kafka import KafkaException
from confluent_kafka.admin import ConfigResource
from deepmerge import always_merger
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

    def describe_brokers_info(self, brokers_metadata, timeout=10):
        """
        Describe information for all Kafka Brokers.

        Args:
            brokers_metadata (dict): A dictionary of broker names and their type.

        Returns:
            dict: The information details for the specified brokers.
        
        Raises:
            KafkaError: If there is an error during the describe process.
        """
        brokers = {"info": {}}
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
        
        brokers["info"]["topics"] = len(topics)
        brokers["info"]["partitions"] = len(partitions)
        brokers["info"]["replicas"] = len(replicas)
        brokers["info"]["consumer_groups"] = len(groups)

        return brokers
    
    def describe_brokers_config(self, brokers_metadata, timeout=10):
        """
        Describe configuration for all Kafka Brokers.

        Args:
            brokers_metadata (dict): The default cluster configuration for the brokers.

        Returns:
            dict: The default cluster configuration for the brokers.
        
        Raises:
            KafkaError: If there is an error during the describe process.
        """
        broker_id = str(list(brokers_metadata.brokers.keys())[0])
        resources = [ConfigResource("broker", broker_id)]
        future = self.admin_client.describe_configs(resources)

        broker_configs = {}
        for res, f in future.items():
            configs = f.result()

            broker_configs = {}
            broker_configs["config"] = {config.name: config.value for config in configs.values()}
        
        return broker_configs

    def describe(self, info=True, config=False, timeout=10):
        """
        Describe one or many Kafka Brokers.

        Args:
            info (bool, optional): Whether to show topic info in the output. Defaults to True.
            config (bool, optional): Whether to show topic configuration in the output. Defaults to False.
            timeout (int, optional): The time (in seconds) to wait for the operation to complete before timing out.

        Returns:
            dict: The broker metadata, including info and/or config. An error dictionary if both info or config are False.
        
        Raises:
            Exception: If neither info or config are specified.
            KafkaError: If there is an error during the describe process.
        """
        if not (info or config):
            raise Exception("Failed to describe brokers. One of info and/or config is required.")

        brokers_metadata = self.admin_client.list_topics(timeout=timeout)
        
        brokers = {}
        if info:
            brokers_info = self.describe_brokers_info(brokers_metadata, timeout=timeout)
            brokers = always_merger.merge(brokers, brokers_info)

        if config:
            brokers_config = self.describe_brokers_config(brokers_metadata, timeout=timeout)
            brokers = always_merger.merge(brokers, brokers_config)

        return brokers

        
    def alter(self):
        raise NotImplemented

    def delete(self):
        raise NotImplemented