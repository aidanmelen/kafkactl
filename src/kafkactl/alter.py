from kafka import (Cluster, Topic,ConsumerGroup, Acl, Consumer, Producer)

import click
import configparser
import json

@click.group("alter")
@click.pass_obj
def alter(admin_client):
    """Alter one or many resources."""
    pass

@alter.command("acl")
@click.pass_obj
def alter_acl(ctx):
    """Alter Kafka ACL."""
    raise NotImplemented

@alter.command("cluster")
@click.pass_obj
def alter_brokers(ctx):
    """Alter Kafka Brokers."""
    raise NotImplemented

@alter.command("group")
@click.pass_obj
def alter_consumer_group(ctx):
    """Alter Kafka Consumer Group."""
    raise NotImplemented

@alter.command("topic")
@click.argument("topic")
@click.option("--filename", "-f", metavar="PATH", type=click.File("r"), help="Path to the properties file containing configs.")
@click.option("configs", "--config", "-c", metavar="NAME=VALUE", type=str, multiple=True, help="Configuration in NAME=VALUE format.")
@click.pass_obj
def alter_topic(ctx, topic, filename, configs):
    """Alter Kafka Topic."""
    config_data = {}
    parser = configparser.ConfigParser()

    if filename:
        parser.read_string('[default]\n' + filename.read())

    elif configs:
        parser.read_string('[default]\n' + '\n'.join(configs))

    config_data = {key: parser['default'][key] for key in parser['default']}

    admin_client = ctx.get("admin_client")
    t = Topic(admin_client)
    results = t.alter(topic, config_data)
    if results:
        click.echo(json.dumps(results, sort_keys=True))