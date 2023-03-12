from confluent_kafka.admin import NewTopic, ConfigResource
from confluent_kafka import KafkaException, KafkaError

from .kafka_resource import KafkaResource
from .cluster import Cluster

class Topic(KafkaResource):
    def __init__(self, admin_client):
        """
        The Kafka Topic wrapper class.

        Args:
            admin_client (kafka.admin.client.AsyncAdminClient): The Kafka AdminClient instance.
        """
        super().__init__(admin_client=admin_client)
        
    def get(self, show_internal=False, timeout=10):
        """
        Get Kafka Topics.

        Args:
            show_internal (bool, optional): Whether to show internal topics. Defaults to True.
            timeout (int, optional): The time (in seconds) to wait for the operation to complete before timing out.

        Returns:
            List[str]: A list of Kafka Topic metadata.
        
        Raises:
            KafkaError: If there is an error during the list process.
        """
        topics_metadata = self.admin_client.list_topics(timeout=timeout)
        results = [{"name": str(topic), "partitions": len(topic.partitions), } for topic in topics_metadata.topics.values()]
        
        if not show_internal:
            results = [t for t in results if not (t["name"].startswith("__") or t["name"].startswith("_confluent"))]

        return results
    
    def get_configs(self, topics=None, timeout=10):
        """
        Get configuration for one or many Kafka Topics.

        Args:
            topics (list, optional): List of topics to describe. If None, all topics are described. Defaults to None.
            timeout (int, optional): The time (in seconds) to wait for the operation to complete before timing out.

        Returns:
            dict: The configuration for the specified Kafka Topics.
        
        Raises:
            KafkaError: If there is an error during the get configurations process.
        """
        # List all topics metadata when the topics argument is None
        if topics:
            response_metadata = {
                topic: metadata for topic, metadata in self.admin_client.list_topics(timeout=timeout).topics.items() if topic in topics
            }
        else:
            response_metadata = self.admin_client.list_topics(timeout=timeout).topics

        results = {}
        
        # Get the topic config(s)
        resources = [ConfigResource("topic", t) for t in response_metadata.keys()]
        if not resources:
            raise KafkaException(KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART, f"The topic '{topics[0]}' does not exist."))

        # Loop through each resource and its corresponding future
        future = self.admin_client.describe_configs(resources)

        for topic, f in future.items():
            response_metadata = f.result()
            results[topic.name] = {}
            results[topic.name] = {m.name: m.value if m.value != "" and m.value != None else "-" for m in response_metadata.values()}

        return results

    def create(self, topic, partitions, replication_factor, config_data={}):
        """
        Create one or many Kafka Topics.

        Args:
            topic (str): A topic name to be created.
            partitions (int): The number of partitions per topic.
            replication_factor (int): The number of replicas per partition.
            config_data (dict): Configuration data for the topic.

        Returns:
            None
        
        Raises:
            KafkaError: If there is an error during the creation process.
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
            dict: The information for the specified Kafka Topics.
        
        Raises:
            KafkaError: If there is an error during the describe process.
        """
        # List all topics metadata when the topics argument is not set
        if topics:
            topics_metadata = {
                topic: metadata for topic, metadata in self.admin_client.list_topics(timeout=timeout).topics.items() if topic in topics
            }
        else:
            topics_metadata = self.admin_client.list_topics(timeout=timeout).topics

        results = {}
        # Loop over topics.
        for topic_name, topic in topics_metadata.items():
            results[topic_name] = {}
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

            results[topic_name]["partitions"] = len(partitions)
            results[topic_name]["replicas"] = len(replicas)
            results[topic_name]["availability"] = partitions
        
        return results

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
