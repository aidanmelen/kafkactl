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
@click.option("states", "--state", "-s", type=click.Choice(["STABLE", "EMPTY"], case_sensitive=False), default=["STABLE"], multiple=True, metavar="STATES", help="Only list consumer groups which are currently in these states.")
@click.option("--show-simple", "-S", default=False, is_flag=True, help="Show consumer groups which are type simple.")
@click.option("--timeout", "-T", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.option("--output", "-o", type=click.Choice(["TABULATE", "JSON"], case_sensitive=False), default="TABULATE", metavar="FORMAT", help="The output format.")
@click.pass_obj
def list_consumer_groups(ctx, states, show_simple, timeout, output):
    """List Kafka Consumer Groups."""
    group = ConsumerGroup(ctx.get("admin_client"))
    groups = group.list(states=states, show_simple=show_simple, timeout=timeout)

    if output.upper() == "TABULATE":
        headers=["NAME", "TYPE", "STATE"]
        group_rows = [[g["name"], g["type"].capitalize(), g["state"].capitalize()] for g in groups]
        click.echo(tabulate(group_rows, headers=headers, tablefmt="plain"))
    
    if output.upper() == "JSON":
        click.echo(json.dumps(groups))

@list.command("topics")
@click.option("--timeout", "-T", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.option("--output", "-o", type=click.Choice(["TABULATE", "JSON"], case_sensitive=False), default="TABULATE", metavar="FORMAT", help="The output format.")
@click.pass_obj
def list_topics(ctx, timeout, output):
    """List Kafka topics."""
    topic = Topic(ctx.get("admin_client"))
    topics = topic.list(timeout=timeout)
    
    if output.upper() == "TABULATE":
        headers=["NAME", "PARTITIONS"]
        topic_rows = [[t["name"], t["partitions"]] for t in topics]
        click.echo(tabulate(topic_rows, headers=headers, tablefmt="plain"))
    
    if output.upper() == "JSON":
        click.echo(json.dumps(topics))
