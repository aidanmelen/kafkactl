from .kafka_resource import KafkaResource

class Consumer(KafkaResource):
    def __init__(self, admin_client):
        """
        The Kafka Consumer class.

        Args:
            admin_client (kafka.admin.client.AsyncAdminClient): The Kafka AdminClient instance.
        """
        super().__init__(admin_client=admin_client)

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