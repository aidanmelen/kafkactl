from confluent_kafka import (KafkaException, ConsumerGroupTopicPartitions,
                             ConsumerGroupState)
from .kafka_resource import KafkaResource

class ConsumerGroup(KafkaResource):
    def __init__(self, admin_client):
        """
        Initialize a new instance of the Consumer Group class.

        Args:
            admin_client (kafka.admin.client.AsyncAdminClient): The Kafka AdminClient instance.
        """
        super().__init__(admin_client=admin_client)
        
    def list(self, states=["STABLE", "EMPTY"], timeout=10):
        """
        List Kafka Consumer Groups.

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

            consumer_groups = []
            for group in groups.valid:
                consumer_groups.append({
                    "name": group.group_id,
                    "type": "simple" if group.is_simple_consumer_group else "high-level",
                    "state": group.state.name.lower(),
                })
            
            # for error in groups.errors:
            #     print(error)

            return consumer_groups

        except Exception as e:
            error_msg = {"error": str(e), "message": "Failed to list consumer groups."}
            return error_msg

    def create(self):
        raise NotImplemented
    
    def describe(self):
        raise NotImplemented
        
    def alter(self):
        raise NotImplemented

    def delete(successful):
        raise NotImplemented