from kafka.topic import Topic

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
@click.pass_obj
def describe_brokers(ctx):
    """Describe Kafka Brokers."""
    raise NotImplemented

@describe.command("groups")
@click.pass_obj
def describe_consumer_groups(ctx):
    """Describe Kafka Consumer Groups."""
    raise NotImplemented

@describe.command("topics")
@click.option("topics", "--topic", "-t", multiple=True, metavar="TOPIC", help="The name of the Kafka topic. This option can be used multiple times to specify multiple topics.")
@click.option("--status", "-s", default=False, is_flag=True, help="Whether to retrieve status information about the topics.")
@click.option("--config", "-c", default=False, is_flag=True, help="Whether to retrieve configuration information about the topics.")
@click.pass_obj
def describe_topics(ctx, topics, status, config):
    """Describe Kafka topics."""
    if not topics:
        raise click.UsageError("At least one topic must be specified with --topic")

    if status or config:
        topic = Topic(ctx.get("admin_client"))
        click.echo(json.dumps(topic.describe(topics, status=status, config=config)))
    else:
        raise click.UsageError("One of --status and/or --config is required")