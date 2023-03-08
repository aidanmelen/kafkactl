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

    def create(self):
        raise NotImplemented
    
    def describe(self, groups=None, timeout=10):
        """
        Describe Kafka Consumer Groups.

        Args:
            groups (list[str]): The list of consumer group names to be described.
            timeout (int, optional): The time (in seconds) to wait for the operation to complete before timing out.

        Returns:
            A dictionary with consumer group names as keys and corresponding ConsumerGroupDescription objects as values.
            If the description of any consumer group fails, an error message will be included in the dictionary value.
        """
        if not groups:
            groups = [group["name"] for group in self.list(timeout=timeout)]

        future = self.admin_client.describe_consumer_groups(groups, request_timeout=timeout)
        groups = {}

        for group_id, f in future.items():
            group_metadata = f.result()
            members = []
            for member in group_metadata.members:
                member_dict = {
                    "id": member.member_id,
                    "host": member.host,
                    "client_id": member.client_id,
                    "group_instance_id": member.group_instance_id,
                    "assignments": [(tp.topic, tp.partition) for tp in member.assignment.topic_partitions] if member.assignment else []
                }
                members.append(member_dict)

            groups[group_id] = {
                "group_id": group_metadata.group_id,
                "is_simple_consumer_group": group_metadata.is_simple_consumer_group,
                "state": group_metadata.state.name.lower(),
                "partition_assignor": group_metadata.partition_assignor,
                "coordinator": {
                    "id": group_metadata.coordinator.id,
                    "host": group_metadata.coordinator.host,
                    "port": group_metadata.coordinator.port
                },
                "members": members
            }
        
        return groups
        
    def alter(self):
        raise NotImplemented

    def delete(self, consumer_groups, timeout=30):
        """
        Delete Kafka Consumer Groups.

        Args:
            consumer_groups (list[str]): The list of consumer group names to be deleted.
            timeout (int, optional): The time (in seconds) to wait for the operation to complete before timing out.

        Returns:
            None
        """
        future = self.admin_client.delete_consumer_groups(consumer_groups, request_timeout=timeout)

        # Wait for operation to finish.
        for group_id, f in future.items():
            try:
                f.result()
                print("Deleted group with id '" + group_id + "' successfully")
            except KafkaException as e:
                print("Error deleting group id '{}': {}".format(group_id, e))