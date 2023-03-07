from tabulate import tabulate
from kafka import (Topic, Broker, Topic, ConsumerGroup, Acl, Consumer, Producer)

import click
import json

@click.group("list")
@click.pass_obj
def list(ctx):
    """List one or many resources."""
    pass

@list.command("acls")
@click.pass_obj
def list_acls(ctx):
    """List Kafka ACLs."""
    raise NotImplemented

@list.command("brokers")
@click.pass_obj
def list_brokers(ctx):
    """List Kafka Brokers."""
    raise NotImplemented

@list.command("groups")
@click.option("states", "--states", "-s", default=["STABLE", "EMPTY"], multiple=True, metavar="STATES", help="Only list consumer groups which are currently in these states.")
@click.option("--timeout", "-to", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.pass_obj
def list_consumer_groups(ctx, states, timeout):
    """List Kafka Consumer Groups."""
    groups_metadata = ConsumerGroup(ctx.get("admin_client"))
    groups = groups_metadata.list(states=states, timeout=timeout)
    group_names = [group for group in groups.keys()]
    click.echo(json.dumps(group_names))

@list.command("topics")
@click.option("--timeout", "-to", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.pass_obj
def list_topics(ctx, timeout):
    """List Kafka topics."""
    topics_metadata = Topic(ctx.get("admin_client"))
    topics = topics_metadata.list(timeout=timeout)
    click.echo(json.dumps(topics))
