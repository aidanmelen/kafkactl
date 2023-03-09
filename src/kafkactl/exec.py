from kafka import (Topic, Broker, Topic, ConsumerGroup, Acl, Consumer, Producer)

import click
import json

@click.group("exec")
@click.pass_obj
def exec(ctx):
    """Exec an operation one or many resources."""
    pass

@exec.command("group")
@click.argument("group")
@click.pass_obj
def execute_consumer_group_offsets(ctx, group):
    """Execute on Kafka Consumer Groups Offsets."""
    # reset offsets?
    raise NotImplemented