from kafka import (Topic, Broker, Topic, ConsumerGroup, Acl, Consumer, Producer)

import click
import json

@click.group("delete")
@click.pass_obj
def delete(ctx):
    """Delete one or many resources."""
    pass

@delete.command("acls")
@click.pass_obj
def delete_acls(ctx):
    """Delete Kafka ACLs."""
    raise NotImplemented

@delete.command("groups")
@click.pass_obj
def delete_consumer_groups(ctx):
    """Delete Kafka Consumer Groups."""
    raise NotImplemented

@delete.command("topics")
@click.option("topics", "--topic", "-t", multiple=True, metavar="TOPIC", help="The name of the Kafka topic. This option can be used multiple times to specify multiple topics.")
@click.option("--timeout", "-T", default=30, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.pass_obj
def delete_topics(ctx, topics, timeout):
    """Delete Kafka topics."""
    if not topics:
        raise click.UsageError("At least one topic must be specified with --topic")

    topic = Topic(ctx.get("admin_client"))
    results = topic.delete(topics, timeout)
    if results:
        click.echo(json.dumps(results))