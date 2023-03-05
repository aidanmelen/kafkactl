from confluent_kafka import (KafkaException, ConsumerGroupTopicPartitions,
                             TopicPartition, ConsumerGroupState)
from confluent_kafka.admin import (AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource,
                                   AclBinding, AclBindingFilter, ResourceType, ResourcePatternType, AclOperation,
                                   AclPermissionType)
from deepmerge import always_merger

class Topic():
    def __init__(self, admin_client):
        """
        Initialize a new instance of the Topic class.

        Args:
            admin_client (kafka.admin.client.AsyncAdminClient): The Kafka AdminClient instance.
        """
        self.admin_client = admin_client

    def _list(self):
        """
        List Kafka topic metadata.

        Returns:
            dict: A dictionary representing the list of topic metadata.
        """
        return self.admin_client.list_topics().topics
        
    def list(self):
        """
        List Kafka topic names.

        Returns:
            List[str]: A list of topic names if successful, otherwise a dictionary with the following keys:
                - error (str): A description of the error that occurred.
                - message (str): A message indicating which topic(s) failed to be created.
        """
        try:
            topic_names = [topic for topic in self._list().keys()]
            return topic_names
        except Exception as e:
            error_msg = {"error": str(e).strip("'"), "message": f"Failed to list topics: {', '.join(topics)}"}
            return error_msg

    def create(self, topics, partitions, replication_factor, config_data=None):
        """
        Create one or many Kafka topics.

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
        fs = self.admin_client.create_topics(new_topics)

        # Wait for operation to finish.
        # Timeouts are preferably controlled by passing request_timeout=15.0
        # to the create() call.
        # All futures will finish at the same time.
        for topic, f in fs.items():
            try:
                return f.result()
            except Exception as e:
                error_msg = {"error": str(e).strip("'"), "message": f"Failed to create topics: {', '.join(topics)}"}
                return error_msg
            
    def _describe_topics_status(self, topics=None):
        """
        Describe status for one or many Kafka topics.

        Args:
            topics (dict): A dictionary of topic names and their partitions. Defaults to None.
        
        Returns:
            dict: A dictionary containing status details for the specified topics.
        """
        topics_status = {}
        try:
            # Loop over topics.
            for topic_name, topic in topics.items():
                topics_status[topic_name] = {"status": {}}
                partitions = []

                # Loop over all partitions of this topic.
                for partition in topic.partitions.values():
                    replicas = []
                    isrs = []

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

                topics_status[topic_name]["status"]["partitions"] = len(partitions)
                topics_status[topic_name]["status"]["availability"] = partitions
                
        except Exception as e:
            error_msg = {"error": str(e), "message": f"Failed to describe topic: {', '.join(topics)}"}
            return error_msg

        return topics_status
    
    def _describe_topics_config(self, topics=None):
        """
        Describe configuration for one or many Kafka topics.

        Args:
            topics (dict): A dictionary of topic names and their partitions. Defaults to None.

        Returns:
            dict: A dictionary containing configuration details for the specified topics.
        """
        try:
            # Get the topic configurations
            resources = [ConfigResource("topic", topic) for topic in topics.keys()]
            fs = self.admin_client.describe_configs(resources)

            # Loop through each resource and its corresponding future
            topic_configs = {}
            for res, f in fs.items():
                topic_configs[res.name] = {"config": {}}

                # Get the result of the future containing the topic configuration
                configs = f.result()

                # Loop through each configuration parameter and add it to the dictionary
                topic_configs[res.name]["config"] = {config.name: config.value for config in configs.values()}

        except Exception as e:
            error_msg = {"error": str(e), "message": f"Failed to describe topic configurations: {', '.join(topics)}"}
            return error_msg

        return topic_configs
        

    def describe(self, topics=None, status=True, config=True):
        """
        Describe one or many Kafka topics.

        Args:
            topics (list, optional): List of topics to describe. If None, all topics are described. Defaults to None.
            status (bool, optional): Whether to include topic status in the output. Defaults to True.
            config (bool, optional): Whether to include topic configuration in the output. Defaults to True.

        Returns:
            dict: A dictionary representing the topic metadata, including status and/or config. An error dictionary if both status or config are False.
        """
        if not (status or config):
            error_msg = {"error": str(e), "message": f"Failed to describe topic configurations: {', '.join(topics)}. One of 'status' or 'config' is required."}
            return error_msg

        # List all topics when the topics argument is not set
        if topics:
            topics = {topic_name: topic for topic_name, topic in self._list().items() if topic_name in topics}
        else:
            topics = self._list()

        # Get topic status and config and deep merge the resulting dictionaries
        topics_metadata = {}
        if status:
            topics_status = self._describe_topics_status(topics)
            topics_metadata = always_merger.merge(topics_metadata, topics_status)

        if config:
            topics_config = self._describe_topics_config(topics)
            topics_metadata = always_merger.merge(topics_metadata, topics_config)

        return topics_metadata
        
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
                
            fs = self.admin_client.alter_configs(resources)
                
            # Wait for operation to finish.
            for res, f in fs.items():
                return f.result()  # empty, but raises exception on failure

        except Exception as e:
            error_msg = {"error": str(e), "message": f"Failed to alter topic configurations: {', '.join(topics)}"}
            return error_msg

    def delete(self, topics, timeout=30):
        """
        Delete one or many Kafka topics.
        
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
        fs = self.admin_client.delete_topics(list(topics), operation_timeout=timeout)

        # Wait for operation to finish.
        for topic, f in fs.items():
            try:
                return f.result()
            except Exception as e:
                error_msg = {"error": e, "message": f"Failed to delete topics: {', '.join(topics)}"}
                return error_msg
