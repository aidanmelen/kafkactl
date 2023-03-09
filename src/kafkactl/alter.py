from kafka import (Topic, Broker, Topic, ConsumerGroup, Acl, Consumer, Producer)

import click
import json

@click.group("alter")
@click.pass_obj
def alter(admin_client):
    """Alter one or many resources."""
    pass

@alter.command("acl")
@click.pass_obj
def alter_acl(ctx):
    """Alter Kafka ACL."""
    raise NotImplemented

@alter.command("brokers")
@click.pass_obj
def alter_brokers(ctx):
    """Alter Kafka Brokers."""
    raise NotImplemented

@alter.command("group")
@click.pass_obj
def alter_consumer_group(ctx):
    """Alter Kafka Consumer Group."""
    raise NotImplemented

@alter.command("topic")
@click.argument("topic")
@click.option("--config-file", "-f", metavar="PATH", type=click.File("r"), help="Path to the configuration file in JSON format.")
@click.option("--config-data", "-d", metavar="JSON", help="Inline configuration data in JSON format.")
@click.pass_obj
def alter_topic(ctx, topic, config_file, config_data):
    """Alter Kafka Topic."""
    if config_file:
        config_data = json.load(config_file)
    elif config_data:
        config_data = json.loads(config_data)
    else:
        config_data = {}

    admin_client = ctx.get("admin_client")
    t = Topic(admin_client)
    results = t.alter([topic], config_data)
    if results:
        click.echo(json.dumps(results, sort_keys=True))