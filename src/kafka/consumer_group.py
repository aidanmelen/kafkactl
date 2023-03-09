from confluent_kafka import (KafkaException, ConsumerGroupTopicPartitions,
                             ConsumerGroupState, Consumer, TopicPartition,
                             OFFSET_INVALID)
from deepmerge import always_merger
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
                "state": group.state.name,
            })
        
        # for error in groups.errors:
        #     print(error)

        return consumer_groups

    def create(self, bootstrap_servers, group):
        raise NotImplemented

    def get_offsets(self, brokers: str, group: str, topics: str) -> dict:
        # Create consumer.
        # This consumer will not join the group, but the group.id is required by
        # committed() to know which group to get offsets for.
        consumer = Consumer({'bootstrap.servers': brokers, 'group.id': group})

        result = {}

        # Query committed offsets for this group and the given partitions
        committed = consumer.committed(topics, timeout=10)

        for partition in committed:
            # Get the partitions low and high watermark offsets.
            (lo, hi) = consumer.get_watermark_offsets(partition, timeout=10, cached=False)

            if partition.offset == OFFSET_INVALID:
                current_offset = "-"
            else:
                current_offset = "%d" % (partition.offset)

            if hi < 0:
                lag = "no hwmark"  # Unlikely
            elif partition.offset < 0:
                # No committed offset, show total message count as lag.
                # The actual message count may be lower due to compaction
                # and record deletions.
                lag = "%d" % (hi - lo)
            else:
                lag = "%d" % (hi - partition.offset)

            result[partition.topic] = {}
            result[partition.topic][partition.partition] = {
                "current_offset": current_offset,
                "log_end_offset": hi,
                "lag": lag
            }

        consumer.close()

        return result
   
    def describe(self, groups=None, brokers=None, timeout=10):
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
        
        groups_info = {}

        # Describe consumer groups
        for group_id, f in future.items():
            group_metadata = f.result()
            members = []
            for m in group_metadata.members:

                topic_partitions = []
                offsets = []
                if m.assignment:

                    for tp in m.assignment.topic_partitions:

                        offsets = self.get_offsets(brokers, group_id, [tp])

                        topic_partitions.append({
                            "topic": tp.topic,
                            "partition": tp.partition,
                            "current_offset": offsets[tp.topic][tp.partition]["current_offset"],
                            "log_end_offset": offsets[tp.topic][tp.partition]["log_end_offset"],
                            "lag": offsets[tp.topic][tp.partition]["lag"]
                        })


                member = {
                    "id": m.member_id,
                    "host": m.host,
                    "client_id": m.client_id,
                    "group_instance_id": m.group_instance_id,
                    "assignments": topic_partitions,
                }
                members.append(member)


            groups_info[group_id] = {
                "is_simple_consumer_group": group_metadata.is_simple_consumer_group,
                "state": group_metadata.state.name,
                "partition_assignor": group_metadata.partition_assignor,
                "coordinator": {
                    "id": group_metadata.coordinator.id,
                    "host": group_metadata.coordinator.host,
                    "port": group_metadata.coordinator.port
                },
                "members": members,
            }

        return groups_info
        
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

        for group_id, f in future.items():
            f.result()