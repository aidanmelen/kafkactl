from kafka import (Topic, Broker, Topic, ConsumerGroup, Acl, Consumer, Producer)

import click
import json

@click.group("create")
@click.pass_obj
def create(ctx):
    """Create one or many resources."""
    pass

@create.command("acls")
@click.pass_obj
def create_acls(ctx):
    """Create Kafka ACLs."""
    raise NotImplemented

@create.command("groups")
@click.pass_obj
def create_consumer_groups(ctx):
    """Create Kafka Consumer Groups."""
    raise NotImplemented

@create.command("topics")
@click.option("topics", "--topic", "-t", multiple=True, metavar="TOPIC", help="The name of the Kafka topic. This option can be used multiple times to specify multiple topics.")
@click.option("--partitions", "-p", default=3, metavar="PARTITIONS", type=int, help="The number of partitions of the Kafka topic.")
@click.option("--replication-factor", "-r", default=3, metavar="REPLICATION_FACTOR", help="The replication factor of the Kafka topic.")
@click.option("--config-file", "-f", metavar="PATH", type=click.File("r"), help="Path to the configuration file in JSON format.")
@click.option("--config-data", "-d", metavar="JSON", help="Inline configuration data in JSON format.")
@click.pass_obj
def create_topics(ctx, topics, partitions, replication_factor, config_file, config_data):
    """Create Kafka topic."""
    if not topics:
        raise click.UsageError("At least one topic must be specified with --topic")
        
    if config_file:
        config_data = json.load(config_file)
    elif config_data:
        config_data = json.loads(config_data)
    else:
        config_data = {}

    admin_client = ctx.get("admin_client")
    topic = Topic(admin_client)
    results = topic.create(topics, partitions, replication_factor, config_data)
    if results:
        click.echo(json.dumps(results))