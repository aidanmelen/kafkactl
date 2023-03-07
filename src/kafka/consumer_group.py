from confluent_kafka import (KafkaException, ConsumerGroupTopicPartitions,
                             ConsumerGroupState)
# from confluent_kafka.admin import (AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource,
#                                    AclBinding, AclBindingFilter, ResourceType, ResourcePatternType, AclOperation,
#                                    AclPermissionType)

class ConsumerGroup():
    def __init__(self, admin_client):
        """
        Initialize a new instance of the Consumer Group class.

        Args:
            admin_client (kafka.admin.client.AsyncAdminClient): The Kafka AdminClient instance.
        """
        self.admin_client = admin_client
        
    def list(self, states=["STABLE", "EMPTY"], timeout=10):
        """
        List Kafka Consumer Group names.

        Args:
            states (list[str], optional): only list consumer groups which are currently in these states.
            timeout (int, optional): The time (in seconds) to wait for the operation to complete before timing out.

        Returns:
            List[str]: A list of consumer group names if successful, otherwise a dictionary with the following keys:
                - error (str): A description of the error that occurred.
                - message (str): A message indicating which consumer group(s) failed to be listed.
        """
        try:
            states = {ConsumerGroupState[state.upper()] for state in states}
            future = self.admin_client.list_consumer_groups(states=states, request_timeout=timeout)
            groups = future.result()

            consumer_groups = {}
            for group in groups.valid:
                if group.group_id == "":
                    continue

                consumer_groups[group.group_id] = {
                    "type": "simple" if group.is_simple_consumer_group else "high-level",
                    "state": group.state.name,
                }
            
            # for error in groups.errors:
            #     print(error)

            return consumer_groups

        except Exception as e:
            error_msg = {"error": str(e), "message": "Failed to list consumer groups."}
            return error_msg

    def create(self, topics, partitions, replication_factor, config_data=None):
        raise NotImplemented
    
    def describe(self, topics=None, info=True, config=True):
        raise NotImplemented
        
    def alter(self, topics, config_data):
        raise NotImplemented

    def delete(self, topics, timeout=30):
        raise NotImplemented