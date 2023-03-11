from kafka import (Topic, Broker, Topic, ConsumerGroup, Acl, Consumer, Producer)

import click
import json

@click.group("create")
@click.pass_obj
def create(ctx):
    """Create a resource."""
    pass

@create.command("acl")
@click.argument("resource_type")
@click.argument("resource_name")
@click.option("--principal", "-p", metavar="JSON", help="The principal for the ACL.")
@click.option("--permission-type", "-P", metavar="JSON", help="The permission type for the ACL.")
@click.option("--timeout", "-T", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.pass_obj
def create_acl(ctx, resource_type, resource_name, principal, permission_type, timeout):
    """Create a Kafka ACL."""
    admin_client = ctx.get("admin_client")
    a = Acl(admin_client)
    result = a.create(resource_type, resource_name, principal=principal, permission_type=permission_type, timeout=timeout)
    click.echo(result)

@create.command("topic")
@click.argument("topic")
@click.option("--partitions", "-p", default=3, metavar="PARTITIONS", type=int, help="The number of partitions of the Kafka topic.")
@click.option("--replication-factor", "-r", default=3, metavar="REPLICATION_FACTOR", help="The replication factor of the Kafka topic.")
@click.option("--config-file", "-f", metavar="PATH", type=click.File("r"), help="Path to the configuration file in JSON format.")
@click.option("--config-data", "-d", metavar="JSON", help="Inline configuration data in JSON format.")
@click.pass_obj
def create_topic(ctx, topic, partitions, replication_factor, config_file, config_data):
    """Create a Kafka Topic."""
    if config_file:
        config_data = json.load(config_file)
    elif config_data:
        config_data = json.loads(config_data)
    else:
        config_data = {}

    admin_client = ctx.get("admin_client")
    t = Topic(admin_client)
    result = t.create(topic, partitions, replication_factor, config_data)
    if result:
        click.echo(json.dumps(result))