
class Consumer():
    def __init__(self, admin_client):
        """
        The Kafka Consumer class.

        Args:
            admin_client (kafka.admin.client.AsyncAdminClient): The Kafka AdminClient instance.
        """
        self.admin_client = admin_client

    def consume(self):
        raise NotImplemented