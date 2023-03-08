from kafka import (Topic, Broker, Topic, ConsumerGroup, Acl, Consumer, Producer)

import click
import json

@click.group("exec")
@click.pass_obj
def exec(ctx):
    """Exec an operation one or many resources."""
    pass

@exec.command("brokers")
@click.pass_obj
def execute_brokers(ctx):
    """Execute Kafka Brokers."""
    # reassign-partitions?
    raise NotImplemented

@exec.command("groups")
@click.option("groups", "--group", "-g", multiple=True, metavar="GROUP", help="The name of the Kafka Consumer Group. This option can be used multiple times to specify multiple Consumer Groups.")
@click.pass_obj
def execute_consumer_groups(ctx, groups):
    """Execute Kafka Consumer Groups."""
    # reset offsets?
    raise NotImplemented