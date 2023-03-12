from tabulate import tabulate
from kafka import (Cluster, Topic,ConsumerGroup, Acl, Consumer, Producer)

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

@describe.command("cluster")
@click.option("--timeout", "-T", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.option("--output", "-o", type=click.Choice(["TABULATE", "JSON"], case_sensitive=False), default="TABULATE", metavar="FORMAT", help="The output format.")
@click.pass_obj
def describe_cluster(ctx, timeout, output):
    """Describe Kafka Cluster."""
    cluster = Cluster(ctx.get("admin_client"))
    results = cluster.describe(timeout=timeout)

    if output.upper() == "TABULATE":
        headers=["BROKERS", "TOPICS", "PARTITIONS", "REPLICAS", "CONSUMER_GROUPS"]
        broker_rows = [
            [results["brokers"], results["topics"], results["partitions"], results["replicas"], results["consumer_groups"]]
        ]
        click.echo(tabulate(broker_rows, headers=headers, tablefmt="plain", numalign="left"))
    
    if output.upper() == "JSON":
        click.echo(json.dumps(results))

@describe.command("groups")
@click.option("groups", "--group", "-g", multiple=True, metavar="GROUP", help="The name of the Kafka Consumer Group. This option can be used multiple times to specify multiple groups.")
@click.option("--timeout", "-T", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.option("--output", "-o", type=click.Choice(["TABULATE", "JSON"], case_sensitive=False), default="TABULATE", metavar="FORMAT", help="The output format.")
@click.pass_obj
def describe_consumer_groups(ctx, groups, timeout, output):
    """Describe Kafka Consumer Groups."""
    group = ConsumerGroup(ctx.get("admin_client"))
    results = group.describe(list(groups), brokers=ctx.get("bootstrap_servers"), timeout=timeout)

    if output.upper() == "TABULATE":
        headers=["GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID"]
        group_rows = []
        for group, metadata in results.items():
            for m in metadata.get("members", []):
                for a in m.get("assignments", []):
                    group_rows.append([
                        group, a["topic"], a["partition"], 
                        a["current_offset"], a["log_end_offset"], a["lag"], 
                        m["id"], m["host"], m["client_id"]
                    ])

        click.echo(tabulate(group_rows, headers=headers, tablefmt="plain", numalign="left"))
    
    if output.upper() == "JSON":
        click.echo(json.dumps(results))

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
        headers=["TOPIC", "STATUS", "PARTITION", "LEADER", "REPLICAS", "IN-SYNC-REPLICAS"]
        topic_rows = []
        for topic, metadata in topics.items():
            for a in metadata.get("availability", []):
                topic_rows.append([
                    topic, a["status"].capitalize(), a["id"], a["leader"], 
                    ",".join([ str(i) for i in a["replicas"]]),
                    ",".join([ str(i) for i in a["isrs"]]),
                ])
            
        click.echo(tabulate(topic_rows, headers=headers, tablefmt="plain", numalign="left"))

    if output.upper() == "JSON":
        click.echo(json.dumps(topics))