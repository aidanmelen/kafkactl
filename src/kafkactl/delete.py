from kafka import (Topic, Cluster, ConsumerGroup, Acl, Consumer, Producer)

import click
import json

@click.group("delete")
@click.pass_obj
def delete(ctx):
    """Delete one or many resources."""
    pass

@delete.command("acl")
@click.pass_obj
def delete_acls(ctx):
    """Delete a Kafka ACL."""
    raise NotImplemented

@delete.command("group")
@click.argument("group")
@click.option("--timeout", "-T", default=30, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.pass_obj
def delete_group(ctx, group, timeout):
    """Delete a Kafka Group."""
    g = ConsumerGroup(ctx.get("admin_client"))
    results = g.delete(group, timeout=timeout)
    if results:
        click.echo(json.dumps(results))

@delete.command("topic")
@click.argument("topic")
@click.option("--timeout", "-T", default=30, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.pass_obj
def delete_topic(ctx, topic, timeout):
    """Delete a Kafka topic."""
    t = Topic(ctx.get("admin_client"))
    results = t.delete(topic, timeout=timeout)
    if results:
        click.echo(json.dumps(results))