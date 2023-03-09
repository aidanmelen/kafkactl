from confluent_kafka import (KafkaException, ConsumerGroupTopicPartitions,
                             TopicPartition, ConsumerGroupState)
from confluent_kafka.admin import (AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource,
                                   AclBinding, AclBindingFilter, ResourceType, ResourcePatternType, AclOperation,
                                   AclPermissionType)

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

    def create(self, topic, partitions, replication_factor, config_data={}):
        """
        Create one or many Kafka Topics.

        Args:
            topic (str): A topic name to be created.
            partitions (int): The number of partitions per topic.
            replication_factor (int): The number of replicas per partition.
            config_data (dict): Configuration data for the topic.

        Returns:
            None if successful, otherwise a dictionary with the following keys:
                - error (str): A description of the error that occurred.
                - message (str): A message indicating which topic(s) failed to be created.
        """
        new_topics = [NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor, config=config_data)]
        future = self.admin_client.create_topics(new_topics)

        for topic, f in future.items():
            return f.result()

    def describe(self, topics=None, timeout=10):
        """
        Describe one or many Kafka Topics.

        Args:
            topics (list, optional): List of topics to describe. If None, all topics are described. Defaults to None.
            timeout (int, optional): The time (in seconds) to wait for the operation to complete before timing out.

        Returns:
            dict: The topic metadata, including info and/or config. An error dictionary if both info or config are False.
        
        Raises:
            KafkaException: These topics do not exist.
            KafkaError: If there is an error during the describe process.
        """
        # List all topics metadata when the topics argument is not set
        if topics:
            topics_metadata = {
                topic: metadata for topic, metadata in self.admin_client.list_topics(timeout=timeout).topics.items() if topic in topics
            }
        else:
            topics_metadata = self.admin_client.list_topics(timeout=timeout).topics

        topics_info = {}
        # Loop over topics.
        for topic_name, topic in topics_metadata.items():
            topics_info[topic_name] = {}
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
                    'status': "HEALTHY" if len(isrs) == len(replicas) else "UNHEALTHY",
                })

            topics_info[topic_name]["partitions"] = len(partitions)
            topics_info[topic_name]["replicas"] = len(replicas)
            topics_info[topic_name]["availability"] = partitions
        
        return topics_info

    def get_configs(self, topics=None, timeout=10):
        """
        Get configuration for one or many Kafka Topics.

        Args:
            topics (list, optional): List of topics to describe. If None, all topics are described. Defaults to None.
            timeout (int, optional): The time (in seconds) to wait for the operation to complete before timing out.

        Returns:
            dict: The configuration for the specified topics.
        
        Raises:
            KafkaError: If there is an error during the describe process.
        """
        # List all topics metadata when the topics argument is not set
        if topics:
            response_metadata = {
                topic: metadata for topic, metadata in self.admin_client.list_topics(timeout=timeout).topics.items() if topic in topics
            }
        else:
            response_metadata = self.admin_client.list_topics(timeout=timeout).topics

        topic_configs = {}

        # Get the topic configurations
        resources = [ConfigResource("topic", t) for t in response_metadata.keys()]
        future = self.admin_client.describe_configs(resources)

        # Loop through each resource and its corresponding future
        topic_configs = {}
        for topic, f in future.items():

            # Get the result of the future containing the topic configuration
            response_metadata = f.result()

            # Loop through each configuration parameter and add it to the dictionary
            topic_configs[topic.name] = {}
            topic_configs[topic.name] = {m.name: m.value if m.value != "" and m.value != None else "-" for m in response_metadata.values()}

        return topic_configs

    def alter(self, topic, config_data):
        """
        Alter configuration atomically for a Kafka Topic, replacing non-specified configuration properties with the cluster default values.

        Args:
            topic (str): The topic name to be altered.
            config_data (Optional[Dict[str, Union[str, int]]]): Configuration data for the topic.

        Returns:
            None
        
        Raises:
            KafkaError: If there is an error during the alteration process.
        """

        resource = ConfigResource("topic", topic)
        for k, v in config_data.items():
            resource.set_config(k, v)

        # resources = []
        # for topic in topics:
        #     resource = ConfigResource("topic", topic)
        #     resources.append(resource)

        #     for k, v in config_data.items():
        #         resource.set_config(k, v)
            
        future = self.admin_client.alter_configs([resource])
            
        # Wait for operation to finish.
        for res, f in future.items():
            return f.result()  # empty, but raises exception on failure

    def delete(self, topic, timeout=30):
        """
        Delete a Kafka Topic.
        
        Args:
            topic (str): The topic name to be deleted.
            timeout (int, optional): The time (in seconds) to wait for the operation to complete before timing out.

        Returns:
            None
        
        Raises:
            KafkaError: If there is an error during the deletion process.
        """
        future = self.admin_client.delete_topics([topic], operation_timeout=timeout)
        
        for topic, f in future.items():
            return f.result()
