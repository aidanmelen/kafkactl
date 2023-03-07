from kafka import (Topic, Broker, Topic, ConsumerGroup, Acl, Consumer, Producer)

import click
import json

@click.group("exec")
@click.pass_obj
def exec(ctx):
    """Exec an operation one or many resources."""
    pass

@exec.command("acls")
@click.pass_obj
def execute_acls(ctx):
    """Execute Kafka ACLs."""
    raise NotImplemented

@exec.command("brokers")
@click.pass_obj
def execute_brokers(ctx):
    """Execute Kafka Brokers."""
    raise NotImplemented

@exec.command("groups")
@click.option("groups", "--group", "-g", multiple=True, metavar="GROUP", help="The name of the Kafka Consumer Group. This option can be used multiple times to specify multiple Consumer Groups.")
@click.pass_obj
def execute_consumer_groups(ctx, groups):
    """Execute Kafka Consumer Groups."""
    raise NotImplemented

@exec.command("topics")
@click.option("topics", "--topic", "-t", multiple=True, metavar="TOPIC", help="The name of the Kafka Topic. This option can be used multiple times to specify multiple Topics.")
@click.pass_obj
def execute_topics(ctx, topic):
    """Execute Kafka topic."""
    raise NotImplemented