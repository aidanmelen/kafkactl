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
@click.option("info", "--show-info/--hide-info", "-i", default=False, is_flag=True, help="Whether to retrieve information about the brokers.")
@click.option("config", "--show-config/--hide-config", "-c", default=False, is_flag=True, help="Whether to retrieve configuration information about the brokers.")
@click.option("--timeout", "-T", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.pass_obj
def describe_brokers(ctx, info, config, timeout):
    """Describe Kafka Brokers."""
    if info or config:
        broker = Broker(ctx.get("admin_client"))
        click.echo(json.dumps(broker.describe(info=info, config=config, timeout=timeout)))
    else:
        raise click.UsageError("One of --info and/or --config is required")

@describe.command("groups")
@click.option("groups", "--group", "-g", multiple=True, metavar="GROUP", help="The name of the Kafka Consumer Group. This option can be used multiple times to specify multiple groups.")
@click.option("--timeout", "-T", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.pass_obj
def describe_consumer_groups(ctx, groups, timeout):
    """Describe Kafka Consumer Groups."""
    group = ConsumerGroup(ctx.get("admin_client"))
    click.echo(json.dumps(group.describe(list(groups), timeout=timeout)))

@describe.command("topics")
@click.option("topics", "--topic", "-t", multiple=True, metavar="TOPIC", help="The name of the Kafka Topic. This option can be used multiple times to specify multiple topics.")
@click.option("info", "--show-info/--hide-info", "-i", default=False, is_flag=True, help="Whether to retrieve information about the topics.")
@click.option("config", "--show-config/--hide-config", "-c", default=False, is_flag=True, help="Whether to retrieve configuration information about the topics.")
@click.option("--timeout", "-T", default=10, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.pass_obj
def describe_topics(ctx, topics, info, config, timeout):
    """Describe Kafka topics."""
    if info or config:
        topic = Topic(ctx.get("admin_client"))
        click.echo(json.dumps(topic.describe(topics, info=info, config=config, timeout=timeout)))
    else:
        raise click.UsageError("One of --info and/or --config is required")