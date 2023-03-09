from tabulate import tabulate
from kafka import (Topic, Broker, Topic, ConsumerGroup, Acl, Consumer, Producer)

import click
import json

@click.group("get")
@click.pass_obj
def get(ctx):
    """Get one or many resources."""
    pass

@get.command("cluster-config")
@click.option("--timeout", "-T", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.option("--output", "-o", type=click.Choice(["TABULATE", "JSON"], case_sensitive=False), default="TABULATE", metavar="FORMAT", help="The output format.")
@click.pass_obj
def get_cluster_config(ctx, timeout, output):
    """Get Kafka Cluster configuration."""
    broker = Broker(ctx.get("admin_client"))
    cluster_config = broker.get_cluster_config(timeout=timeout)

    if output.upper() == "TABULATE":
        headers=["PROPERTY-NAME", "PROPERTY-VALUE"]
        cluster_config_rows = [[k,v if v != "" and v != None else "-"] for k,v in cluster_config.items()]
        click.echo(tabulate(cluster_config_rows, headers=headers, tablefmt="plain"))
    
    if output.upper() == "JSON":
        click.echo(json.dumps(cluster_config))

@get.command("topic-configs")
@click.option("topics", "--topic", "-t", multiple=True, metavar="TOPIC", help="The name of the Kafka Consumer Topic. This option can be used multiple times to specify multiple topics.")
@click.option("--timeout", "-T", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.option("--output", "-o", type=click.Choice(["TABULATE", "JSON"], case_sensitive=False), default="TABULATE", metavar="FORMAT", help="The output format.")
@click.pass_obj
def get_topic_configs(ctx, topics, timeout, output):
    """Get Kafka Topic configurations."""
    topic = Topic(ctx.get("admin_client"))
    topic_configs = topic.get_configs(topics, timeout=timeout)

    if output.upper() == "TABULATE":
        headers=["TOPIC", "PROPERTY-NAME", "PROPERTY-VALUE"]
        for topic, config in topic_configs.items():
            topic_config_rows = [[topic, k, v] for k,v in config.items()]
            click.echo()
            click.echo(tabulate(topic_config_rows, headers=headers, tablefmt="plain"))
    
    if output.upper() == "JSON":
        click.echo(json.dumps(topic_configs))