from .kafka_resource import KafkaResource

class Broker(KafkaResource):
    def __init__(self, admin_client):
        """
        The Kafka Broker class.

        Args:
            admin_client (kafka.admin.client.AsyncAdminClient): The Kafka AdminClient instance.
        """
        super().__init__(admin_client=admin_client)
    
    def list(self, timeout=10):
        metadata = self.admin_client.list_topics(timeout=timeout)

        brokers = []
        for b in iter(metadata.brokers.values()):
            brokers.append({
                "name": f"broker.{b.id}",
                "type": "controller" if b.id ==  metadata.controller_id else "worker"
            })

        return brokers
    
    def create(self):
        raise NotImplemented
    
    def describe(self):
        raise NotImplemented
        
    def alter(self):
        raise NotImplemented

    def delete(successful):
        raise NotImplemented