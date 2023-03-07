from confluent_kafka import (KafkaException, ConsumerGroupTopicPartitions,
                             TopicPartition, ConsumerGroupState)
from confluent_kafka.admin import (AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource,
                                   AclBinding, AclBindingFilter, ResourceType, ResourcePatternType, AclOperation,
                                   AclPermissionType)
from deepmerge import always_merger

from .kafka_resource import KafkaResource

class Topic(KafkaResource):
    def __init__(self, admin_client):
        """
        The Kafka Topic class.

        Args:
            admin_client (kafka.admin.client.AsyncAdminClient): The Kafka AdminClient instance.
        """
        self.admin_client = admin_client
        
    def list(self, timeout=10):
        """
        List Kafka Topics.

        Args:
            timeout (int, optional): The time (in seconds) to wait for the operation to complete before timing out.

        Returns:
            List[str]: A list of topic names if successful, otherwise a dictionary with the following keys:
                - error (str): A description of the error that occurred.
                - message (str): A message indicating which topic(s) failed to be created.
        """
        try:
            topics_metadata = self.admin_client.list_topics(timeout=timeout)
            topics = [{"name": str(topic), "partitions": len(topic.partitions)} for topic in topics_metadata.topics.values()]
            return topics
        except KafkaException as e:
            error_msg = {"error_message": str(e)}
            return error_msg

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
        # Call create_topics to asynchronously create topics, a dict
        # of <topic,future> is returned.
        future = self.admin_client.create_topics(new_topics)

        # Wait for operation to finish.
        # Timeouts are preferably controlled by passing request_timeout=15.0
        # to the create() call.
        # All futures will finish at the same time.
        for topic, f in future.items():
            try:
                return f.result()
            except KafkaException as e:
                error_msg = {"error_message": str(e)}
                return error_msg
    
    # def is_under_replicated(self, topic_name):
    #     topic_metadata = self.admin_client.describe_topics([topic_name])
    #     partitions_metadata = topic_metadata.topics[topic_name].partitions
    #     for partition_metadata in partitions_metadata:
    #         replication_factor = len(partition_metadata.replicas)
    #         in_sync_replicas = len(partition_metadata.isr)
    #         if in_sync_replicas < replication_factor:
    #             return True
    #     return False
            
    def describe_topics_info(self, topics=None):
        """
        Describe info for one or many Kafka Topics.

        Args:
            topics (dict): A dictionary of topic names and their partitions. Defaults to None.
        
        Returns:
            dict: A dictionary containing info details for the specified topics.
        """
        topics_info = {}
        try:
            # Loop over topics.
            for topic_name, topic in topics.items():
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
                topics_info[topic_name]["info"]["availability"] = partitions
        
        except KafkaException as e:
            error_msg = {"error_message": str(e)}
            return error_msg
        except Exception as e:
            error_msg = {"error_message": f"Unknown Error: {e}"}
            return error_msg

        return topics_info
    
    def describe_topics_config(self, topics=None):
        """
        Describe configuration for one or many Kafka Topics.

        Args:
            topics (dict): A dictionary of topic names and their partitions. Defaults to None.

        Returns:
            dict: A dictionary containing configuration details for the specified topics.
        """
        try:
            topic_configs[res.name] = {"config": {}}

            # Get the topic configurations
            resources = [ConfigResource("topic", topic) for topic in topics.keys()]
            future = self.admin_client.describe_configs(resources)

            # Loop through each resource and its corresponding future
            topic_configs = {}
            for res, f in future.items():

                # Get the result of the future containing the topic configuration
                configs = f.result()

                # Loop through each configuration parameter and add it to the dictionary
                topic_configs[res.name]["config"] = {config.name: config.value for config in configs.values()}

        except KafkaException as e:
            error_msg = {"error_message": str(e)}
            return error_msg
        except Exception as e:
            error_msg = {"error_message": f"Unknown Error: {e}"}
            return error_msg

        return topic_configs
        

    def describe(self, topics=None, info=True, config=True):
        """
        Describe one or many Kafka Topics.

        Args:
            topics (list, optional): List of topics to describe. If None, all topics are described. Defaults to None.
            info (bool, optional): Whether to include topic info in the output. Defaults to True.
            config (bool, optional): Whether to include topic configuration in the output. Defaults to True.

        Returns:
            dict: A dictionary representing the topic metadata, including info and/or config. An error dictionary if both info or config are False.
        """
        try:
            if not (info or config):
                error_msg = {"error_message": str(e)}
                return error_msg

            # List all topics when the topics argument is not set
            if topics:
                topics = {topic_name: topic for topic_name, topic in self.admin_client.list_topics().topics.items() if topic_name in topics}
            else:
                topics = self.admin_client.list_topics().topics

            # Get topic info and config and deep merge the resulting dictionaries
            topics_metadata = {}
            if info:
                topics_info = self.describe_topics_info(topics)
                topics_metadata = always_merger.merge(topics_metadata, topics_info)

            if config:
                topics_config = self.describe_topics_config(topics)
                topics_metadata = always_merger.merge(topics_metadata, topics_config)

            return topics_metadata

        except KafkaException as e:
            error_msg = {"error_message": str(e)}
            return error_msg
        except Exception as e:
            error_msg = {"error_message": f"Unknown Error: {e}"}
            return error_msg
        
    def alter(self, topics, config_data):
        """
        Alter configuration atomically for one or many topics, replacing non-specified configuration properties with their default values.

        Args:
            topics (list[str]): A list of topic names to be altered.
            config_data (Optional[Dict[str, Union[str, int]]]): Configuration data for the topic.

        Returns:
            None if successful, otherwise a dictionary with the following keys:
                - error (str): A description of the error that occurred.
                - message (str): A message indicating which topic(s) failed to be altered.
        """
        try:
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

        except KafkaException as e:
            error_msg = {"error_message": str(e)}
            return error_msg
        except Exception as e:
            error_msg = {"error_message": f"Unknown Error: {e}"}
            return error_msg

    def delete(self, topics, timeout=30):
        """
        Delete one or many Kafka Topics.
        
        Args:
            topics (list of str): The list of topic names to be deleted.
            timeout (int, optional): The time (in seconds) to wait for the operation to complete before timing out. Default is 30 seconds.

        Returns:
            None if successful, otherwise a dictionary with the following keys:
                - error (str): A description of the error that occurred.
                - message (str): A message indicating which topic(s) failed to be altered.
        """
        # Call delete_topics to asynchronously delete topics, a future is returned.
        # By default this operation on the broker returns immediately while
        # topics are deleted in the background. But here we give it some time (30s)
        # to propagate in the cluster before returning.
        #
        # Returns a dict of <topic,future>.
        future = self.admin_client.delete_topics(list(topics), operation_timeout=timeout)

        # Wait for operation to finish.
        for topic, f in future.items():
            try:
                return f.result()
            except KafkaException as e:
                error_msg = {"error_message": str(e)}
                return error_msg
