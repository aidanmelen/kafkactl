import unittest
from unittest.mock import MagicMock, call
from kafka.topic import Topic
from confluent_kafka import TopicPartition
from confluent_kafka.admin import NewTopic, ConfigResource, ResourceType


class TestTopic(unittest.TestCase):

    def setUp(self):
        self.admin_client = MagicMock()
        self.topic = Topic(admin_client=self.admin_client)
    
    def test_list(self):
        topics = self.topic.get(timeout=1)

        self.admin_client.assert_has_calls([
            call.list_topics(timeout=1),
            call.list_topics().topics.values(),
            call.list_topics().topics.values().__iter__()
        ])
    
    def test_create(self):
        topic = "topic"
        partitions = 3
        replication_factor = 3
        config = {"cleanup.policy": "compact"}
        topics = self.topic.create(topic, partitions, replication_factor, config)

        self.admin_client.assert_has_calls([
            call.create_topics([NewTopic(
                topic=topic,num_partitions=partitions,
                replication_factor=replication_factor,
                config=config
            )]),
            call.create_topics().items(),
            call.create_topics().items().__iter__()
        ])
    
    def test_describe(self):
        topics = self.topic.describe(topics=["topic1", "topic2"])

        self.admin_client.assert_has_calls([
            call.list_topics(timeout=10),
            call.list_topics().topics.items(),
            call.list_topics().topics.items().__iter__()
        ])

        # assert that  list_topics is called when the topics argument is not specified
        topics = self.topic.describe(timeout=5)

        self.admin_client.assert_has_calls([
            call.list_topics(timeout=5),
            call.list_topics().topics.items(),
            call.list_topics().topics.items().__iter__()
        ])
    
    def test_get_configs(self):
        topics = self.topic.get_configs(topics=["topic1", "topic2"])

        self.admin_client.assert_has_calls([
            # list topics and then filter for the provided topics
            call.list_topics(timeout=10),
            call.list_topics().topics.items(),
            call.list_topics().topics.items().__iter__(),
            call.describe_configs([]),
            call.describe_configs().items(),
            call.describe_configs().items().__iter__()
        ])

        # assert that list_topics is called when the topics argument is not specified
        topics = self.topic.get_configs(timeout=5)

        self.admin_client.assert_has_calls([
            # list topics because there were no provided topics
            call.list_topics(timeout=5),
            call.list_topics().topics.keys(),
            call.list_topics().topics.keys().__iter__(),
            call.describe_configs([]),
            call.describe_configs().items(),
            call.describe_configs().items().__iter__()
        ])
    
    def test_alter(self):
        topic = "topic1"
        config = {"cleanup.policy": "compact"}
        topics = self.topic.alter(topic, config_data=config)

        self.admin_client.assert_has_calls([
            call.alter_configs([ConfigResource(ResourceType.TOPIC,topic)]),
            call.alter_configs().items(),
            call.alter_configs().items().__iter__()
        ])

    def test_delete(self):
        topic = "topic1"
        topics = self.topic.delete(topic)

        self.admin_client.assert_has_calls([
            call.delete_topics(['topic1'], operation_timeout=30),
            call.delete_topics().items(),
            call.delete_topics().items().__iter__()
        ])


if __name__ == "__main__":
    unittest.main()
