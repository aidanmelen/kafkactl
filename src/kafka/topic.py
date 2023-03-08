from confluent_kafka import (KafkaException, ConsumerGroupTopicPartitions,
                             TopicPartition, ConsumerGroupState)
from confluent_kafka.admin import (AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource,
                                   AclBinding, AclBindingFilter, ResourceType, ResourcePatternType, AclOperation,
                                   AclPermissionType)
from deepmerge import always_merger

from .kafka_resource import KafkaResource
from .broker import Broker

class Topic(KafkaResource):
    def __init__(self, admin_client):
        """
        The Kafka Topic class.

        Args:
            admin_client (kafka.admin.client.AsyncAdminClient): The Kafka AdminClient instance.
        """
        super().__init__(admin_client=admin_client)
        
    def list(self, show_internal=False, timeout=10):
        """
        List Kafka Topics.

        Args:
            show_internal (bool, optional): Whether to show internal topics. Defaults to True.
            timeout (int, optional): The time (in seconds) to wait for the operation to complete before timing out.

        Returns:
            List[str]: A list of topic names if successful, otherwise a dictionary with the following keys:
                - error (str): A description of the error that occurred.
                - message (str): A message indicating which topic(s) failed to be created.
        """
        topics_metadata = self.admin_client.list_topics(timeout=timeout)
        topics = [{"name": str(topic), "partitions": len(topic.partitions)} for topic in topics_metadata.topics.values()]
        
        if not show_internal:
            topics = [t for t in topics if not (t["name"].startswith("__") or t["name"].startswith("_confluent"))]

        return topics

    def create(self, topics, partitions, replication_factor, config_data=None):
        """
        Create one or many Kafka Topics.

        Args:
            topics (list[str]): A list of topic names to be created.
            partitions (int): The number of partitions per topic.
            replication_factor (int): The number of replicas per partition.
            config_data (dict): Configuration data for the topic.

        Returns:
            None if successful, otherwise a dictionary with the following keys:
                - error (str): A description of the error that occurred.
                - message (str): A message indicating which topic(s) failed to be created.
        """
        new_topics = [NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor, config=config_data) for topic in topics]
        future = self.admin_client.create_topics(new_topics)

        for topic, f in future.items():
            return f.result()
            
    def describe_topics_info(self, topics_metadata):
        """
        Describe info for one or many Kafka Topics.

        Args:
            topics_metadata (dict): A dictionary of topic names and their partitions.
        
        Returns:
            dict: The info details for the specified topics.
        
        Raises:
            KafkaError: If there is an error during the describe process.
        """
        topics_info = {}
        # Loop over topics.
        for topic_name, topic in topics_metadata.items():
            topics_info[topic_name] = {"info": {}}
            partitions = []

            # Loop over all partitions of this topic.
            for partition in topic.partitions.values():
                replicas = []
                isrs = []
                status = []

                # Loop over all replicas of this partition.
                for broker in partition.replicas:
                    if isinstance(broker, int):
                        replicas.append(broker)
                    else:
                        replicas.append(broker.id)

                # Loop over all in-sync replicas of this partition.
                for broker in partition.isrs:
                    if isinstance(broker, int):
                        isrs.append(broker)
                    else:
                        isrs.append(broker.id)
                
                partitions.append({
                    'id': partition.id,
                    'leader': partition.leader,
                    'replicas': replicas,
                    'isrs': isrs,
                })

            topics_info[topic_name]["info"]["partitions"] = len(partitions)
            topics_info[topic_name]["info"]["replicas"] = len(replicas)
            topics_info[topic_name]["info"]["availability"] = partitions
        
        return topics_info
        
    def describe_topics_config(self, topics_metadata):
        """
        Describe configuration for one or many Kafka Topics.

        Args:
            topics_metadata (dict): A dictionary of topic names and their partitions.

        Returns:
            dict: The configuration for the specified topics.
        
        Raises:
            KafkaError: If there is an error during the describe process.
        """
        topic_configs = {}

        # Get the topic configurations
        resources = [ConfigResource("topic", t) for t in topics_metadata.keys()]
        future = self.admin_client.describe_configs(resources)

        # Loop through each resource and its corresponding future
        topic_configs = {}
        for res, f in future.items():

            # Get the result of the future containing the topic configuration
            configs = f.result()

            # Loop through each configuration parameter and add it to the dictionary
            topic_configs[res.name] = {}
            topic_configs[res.name]["config"] = {config.name: config.value for config in configs.values()}

        return topic_configs

    def describe(self, topics=None, info=True, config=False, timeout=10):
        """
        Describe one or many Kafka Topics.

        Args:
            topics (list, optional): List of topics to describe. If None, all topics are described. Defaults to None.
            info (bool, optional): Whether to show topic info in the output. Defaults to True.
            config (bool, optional): Whether to show topic configuration in the output. Defaults to False.
            timeout (int, optional): The time (in seconds) to wait for the operation to complete before timing out.

        Returns:
            dict: The topic metadata, including info and/or config. An error dictionary if both info or config are False.
        
        Raises:
            Exception: If neither info or config are specified.
            KafkaError: If there is an error during the describe process.
        """
        if not (info or config):
            raise Exception("Failed to describe topics. One of info and/or config is required.")

        # List all topics metadata when the topics argument is not set
        if topics:
            topics_metadata = {
                topic_name: topic for topic_name, topic in self.admin_client.list_topics(timeout=timeout).topics.items() if topic_name in topics
            }
        else:
            topics_metadata = self.admin_client.list_topics(timeout=timeout).topics

        # Get topic info and config and deep merge the resulting dictionaries
        topics = {}
        if info:
            topics_info = self.describe_topics_info(topics_metadata)
            topics = always_merger.merge(topics, topics_info)

        if config:
            topics_config = self.describe_topics_config(topics_metadata)
            topics = always_merger.merge(topics, topics_config)

        return topics

    def alter(self, topics, config_data):
        """
        Alter configuration atomically for one or many topics, replacing non-specified configuration properties with their default values.

        Args:
            topics (list[str]): A list of topic names to be altered.
            config_data (Optional[Dict[str, Union[str, int]]]): Configuration data for the topic.

        Returns:
            None
        
        Raises:
            KafkaError: If there is an error during the alteration process.
        """
        resources = []
        for topic in topics:
            resource = ConfigResource("topic", topic)
            resources.append(resource)

            for k, v in config_data.items():
                resource.set_config(k, v)
            
        future = self.admin_client.alter_configs(resources)
            
        # Wait for operation to finish.
        for res, f in future.items():
            return f.result()  # empty, but raises exception on failure

    def delete(self, topics, timeout=30):
        """
        Delete one or many Kafka Topics.
        
        Args:
            topics (list of str): The list of topic names to be deleted.
            timeout (int, optional): The time (in seconds) to wait for the operation to complete before timing out.

        Returns:
            None
        
        Raises:
            KafkaError: If there is an error during the deletion process.
        """
        future = self.admin_client.delete_topics(list(topics), operation_timeout=timeout)
        
        # Wait for operation to finish.
        for topic, f in future.items():
            return f.result()
