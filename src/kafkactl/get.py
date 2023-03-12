from tabulate import tabulate
from kafka import (Acl, Cluster, ConsumerGroup, Consumer, Producer, Topic)

import click
import json

@click.group("get")
@click.pass_obj
def get(ctx):
    """Get one or many resources."""
    pass

@get.command("acls")
@click.pass_obj
def get_acls(ctx):
    """Get Kafka ACLs."""
    raise NotImplemented

@get.command("cluster")
@click.option("--timeout", "-T", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.option("--output", "-o", type=click.Choice(["TABULATE", "JSON"], case_sensitive=False), default="TABULATE", metavar="FORMAT", help="The output format.")
@click.pass_obj
def get_cluster(ctx, timeout, output):
    """Get Kafka Cluster."""
    cluster = Cluster(ctx.get("admin_client"))
    results = cluster.get(timeout=timeout)

    if output.upper() == "TABULATE":
        headers=["BROKER", "TYPE", "ENDPOINT"]
        rows = [[r["name"], r["type"].capitalize(), r["endpoint"]] for r in results]
        click.echo(tabulate(rows, headers=headers, tablefmt="plain", numalign="left"))
    
    if output.upper() == "JSON":
        click.echo(json.dumps(results))

@get.command("cluster-default-configs")
@click.option("--timeout", "-T", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.option("--output", "-o", type=click.Choice(["TABULATE", "JSON"], case_sensitive=False), default="TABULATE", metavar="FORMAT", help="The output format.")
@click.pass_obj
def get_cluster_default_configs(ctx, timeout, output):
    """Get Kafka Cluster default configuration."""
    cluster = Cluster(ctx.get("admin_client"))
    results = cluster.get_default_configs(timeout=timeout)

    if output.upper() == "TABULATE":
        headers=["NAME", "VALUE"]
        rows = [[k,v if v != "" and v != None else "-"] for k,v in results.items()]
        click.echo(tabulate(rows, headers=headers, tablefmt="plain"))
    
    if output.upper() == "JSON":
        click.echo(json.dumps(results))

@get.command("groups")
@click.option("states", "--state", "-s", type=click.Choice(["STABLE", "EMPTY"], case_sensitive=False), default=["STABLE", "EMPTY"], multiple=True, metavar="STATES", help="Only get consumer groups which are currently in these states.")
@click.option("topics", "--topics", "-t", default=[], multiple=True, metavar="TOPICS", help="Only get consumer groups which are currently consuming from these topics.")
@click.option("--timeout", "-T", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.option("--output", "-o", type=click.Choice(["TABULATE", "JSON"], case_sensitive=False), default="TABULATE", metavar="FORMAT", help="The output format.")
@click.pass_obj
def get_consumer_groups(ctx, states, topics, timeout, output):
    """Get Kafka Consumer Groups."""
    group = ConsumerGroup(ctx.get("admin_client"))
    results = group.get(states=states, topics=topics, timeout=timeout)

    if output.upper() == "TABULATE":
        headers=["GROUP", "TYPE", "STATE"]
        rows = [[r["name"], r["type"].capitalize(), r["state"].capitalize()] for r in results]
        click.echo(tabulate(rows, headers=headers, tablefmt="plain"))
    
    if output.upper() == "JSON":
        click.echo(json.dumps(results))

@get.command("topics")
@click.option("--show-internal/--hide-internal", "-s/-h", default=True, is_flag=True, help="Whether to show internal topics.")
@click.option("--timeout", "-T", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.option("--output", "-o", type=click.Choice(["TABULATE", "JSON"], case_sensitive=False), default="TABULATE", metavar="FORMAT", help="The output format.")
@click.pass_obj
def get_topics(ctx, show_internal, timeout, output):
    """Get Kafka topics."""
    topic = Topic(ctx.get("admin_client"))
    results = topic.get(show_internal=show_internal, timeout=timeout)
    
    if output.upper() == "TABULATE":
        headers=["TOPIC", "PARTITION"]
        rows = [[r["name"], r["partitions"]] for r in results]
        click.echo(tabulate(rows, headers=headers, tablefmt="plain", numalign="left"))
    
    if output.upper() == "JSON":
        click.echo(json.dumps(results))

@get.command("topic-configs")
@click.option("topics", "--topic", "-t", multiple=True, metavar="TOPIC", help="The name of the Kafka Consumer Topic. This option can be used multiple times to specify multiple topics.")
@click.option("--show-cluster-defaults/--hide-cluster-defaults", "-s/-h", default=False, is_flag=True, help="Whether to additionally show cluster default configuration.")
@click.option("--timeout", "-T", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.option("--output", "-o", type=click.Choice(["TABULATE", "JSON"], case_sensitive=False), default="TABULATE", metavar="FORMAT", help="The output format.")
@click.pass_obj
def get_topic_configs(ctx, topics, show_cluster_defaults, timeout, output):
    """Get Kafka Topic configurations."""
    topic = Topic(ctx.get("admin_client"))
    results = topic.get_configs(topics, timeout=timeout)

    if show_cluster_defaults:
        cluster = Cluster(ctx.get("admin_client"))
        results["default"] = cluster.get_default_configs(timeout=timeout)
    
    if output.upper() == "TABULATE":
        if show_cluster_defaults:
            headers=["TOPIC", "NAME", "VALUE", "DEFAULT"]
            rows = []
            for topic, config in results.items():
                if topic != "default":
                    rows.extend([[topic, k, v, results["default"].get(k, "-")] for k,v in config.items()])
        else:
            headers=["TOPIC", "NAME", "VALUE"]
            rows = []
            for topic, config in results.items():
                rows.extend([[topic, k, v] for k,v in config.items()])
        
        click.echo(tabulate(rows, headers=headers, tablefmt="plain"))
    
    if output.upper() == "JSON":
        click.echo(json.dumps(results))