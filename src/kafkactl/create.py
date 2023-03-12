from kafka import (Topic, Topic, ConsumerGroup, Acl, Consumer, Producer)

import click
import configparser
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
@click.option("--replication-factor", "-r", default=3, metavar="REPLICATION_FACTOR", type=int, help="The replication factor of the Kafka topic.")
@click.option("--filename", "-f", metavar="PATH", type=click.File("r"), help="Path to the properties file containing configs.")
@click.option("configs", "--config", "-c", default=[], metavar="NAME=VALUE", type=str, multiple=True, help="Configuration in NAME=VALUE format.")
@click.pass_obj
def create_topic(ctx, topic, partitions, replication_factor, filename, configs):
    """Create a Kafka Topic."""
    config_data = {}
    parser = configparser.ConfigParser()

    if filename:
        parser.read_string('[default]\n' + filename.read())
        config_data = {key: parser['default'][key] for key in parser['default']}

    elif configs:
        parser.read_string('[default]\n' + '\n'.join(configs))
        config_data = {key: parser['default'][key] for key in parser['default']}
    
    else:
        config_data = {}

    admin_client = ctx.get("admin_client")
    t = Topic(admin_client)
    result = t.create(topic, partitions, replication_factor, config_data)
    if result:
        click.echo(json.dumps(result))