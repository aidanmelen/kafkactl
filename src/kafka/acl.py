from confluent_kafka.admin import (AclBinding, AclBindingFilter, AclOperation, AclPermissionType,
                                   ResourceType, ResourcePatternType)
from .kafka_resource import KafkaResource

class Acl(KafkaResource):
    def __init__(self, admin_client):
        """
        The Kafka Access Control List (ACL) wrapper class.

        Args:
            admin_client (kafka.admin.client.AsyncAdminClient): The Kafka AdminClient instance.
        """
        super().__init__(admin_client=admin_client)

    def list(self):
        raise NotImplemented
    
    def create(self, timeout=10):
        raise NotImplemented
       
    def describe(self, resource_type, resource_name, principal=None, permission_type=None, timeout=10):
        """
        Describe the Kafka Access Control Lists (ACLs).

        Args:
            resource_type (kafka.admin.acl.ResourceType): The Kafka resource type.
            resource_name (str): The Kafka resource name.
            principal (str, optional): The principal for the ACL.
            permission_type (str, optional): The permission type for the ACL.
            timeout (int, optional): The time (in seconds) to wait for the operation to complete before timing out.

        Returns:
            None
        """
        # acl_binding_filter = AclBindingFilter(
        #     resource_type,
        #     resource_name,
        #     ResourcePatternType.LITERAL,
        #     principal,
        #     "*",
        #     permission_type,
        #     None
        # )

        # future = self.admin_client.describe_acls(acl_binding_filter, request_timeout=timeout)

        # acl_bindings = future.result()
        # acls = []
        # for acl_binding in acl_bindings:
        #     print(acl_binding)
        # return acls
        pass
        
    def alter(self):
        raise NotImplemented

    def delete(self):
        raise NotImplemented