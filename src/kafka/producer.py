from .kafka_resource import KafkaResource

class Producer(KafkaResource):
    def __init__(self, admin_client):
        """
        The Kafka Producer class.

        Args:
            admin_client (kafka.admin.client.AsyncAdminClient): The Kafka AdminClient instance.
        """
        self.admin_client = admin_client

    def list(self):
        raise NotImplemented
    
    def create(self):
        raise NotImplemented
    
    def describe(self):
        raise NotImplemented
        
    def alter(self):
        raise NotImplemented

    def delete(successful):
        raise NotImplemented