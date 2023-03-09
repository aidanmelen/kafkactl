from tabulate import tabulate
from kafka import (Topic, Broker, Topic, ConsumerGroup, Acl, Consumer, Producer)

import click
import json

@click.group("describe")
@click.pass_obj
def describe(ctx):
    """Describe one or many resources."""
    pass

@describe.command("acls")
@click.pass_obj
def describe_acls(ctx):
    """Describe Kafka ACLs."""
    raise NotImplemented

@describe.command("brokers")
@click.option("--timeout", "-T", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.pass_obj
def describe_brokers(ctx, timeout):
    """Describe Kafka Brokers."""
    broker = Broker(ctx.get("admin_client"))
    click.echo(json.dumps(broker.describe(timeout=timeout)))

@describe.command("groups")
@click.option("groups", "--group", "-g", multiple=True, metavar="GROUP", help="The name of the Kafka Consumer Group. This option can be used multiple times to specify multiple groups.")
@click.option("--timeout", "-T", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.option("--output", "-o", type=click.Choice(["TABULATE", "JSON"], case_sensitive=False), default="TABULATE", metavar="FORMAT", help="The output format.")
@click.pass_obj
def describe_consumer_groups(ctx, groups, timeout, output):
    """Describe Kafka Consumer Groups."""
    group = ConsumerGroup(ctx.get("admin_client"))
    groups = group.describe(list(groups), brokers=ctx.get("bootstrap_servers"), timeout=timeout)

    if output.upper() == "TABULATE":
        headers=["GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID"]
        group_rows = []
        for group, metadata in groups.items():
            for members in metadata.get("members", []):
                for assignment in members.get("assignments", []):
                    group_rows.append([
                        group, assignment["topic"], assignment["partition"], 
                        assignment["current_offset"], assignment["log_end_offset"], assignment["lag"], 
                        members["id"], members["host"], members["client_id"]
                    ])

        click.echo(tabulate(group_rows, headers=headers, tablefmt="plain"))
    
    if output.upper() == "JSON":
        click.echo(json.dumps(groups))

@describe.command("topics")
@click.option("topics", "--topic", "-t", multiple=True, metavar="TOPIC", help="The name of the Kafka Topic. This option can be used multiple times to specify multiple topics.")
@click.option("--timeout", "-T", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.option("--output", "-o", type=click.Choice(["TABULATE", "JSON"], case_sensitive=False), default="TABULATE", metavar="FORMAT", help="The output format.")
@click.pass_obj
def describe_topics(ctx, topics, timeout, output):
    """Describe Kafka topics."""
    topic = Topic(ctx.get("admin_client"))
    topics = topic.describe(topics, timeout=timeout)

    if output.upper() == "TABULATE":
        headers=["TOPIC", "PARTITION", "LEADER", "REPLICAS", "IN-SYNC-REPLICAS"]
        topic_rows = []
        for topic, metadata in topics.items():
            for a in metadata.get("availability", []):
                topic_rows.append([topic, a["id"], a["leader"], str(a["replicas"]), str(a["isrs"])])
            
        click.echo(tabulate(topic_rows, headers=headers, tablefmt="plain"))


    if output.upper() == "JSON":
        click.echo(json.dumps(topics))