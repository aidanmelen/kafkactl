from .topic import Topic

import click
import json

@click.group("alter")
@click.pass_obj
def alter(admin_client):
    """Alter one or many resources."""
    pass

@alter.command("topics")
@click.option("topics", "--topic", "-t", multiple=True, metavar="TOPIC", help="The name of the Kafka topic. This option can be used multiple times to specify multiple topics.")
@click.option("--config-file", "-f", metavar="PATH", type=click.File("r"), help="Path to the configuration file in JSON format.")
@click.option("--config-data", "-d", metavar="JSON", help="Inline configuration data in JSON format.")
@click.pass_obj
def alter_topics(ctx, topics, config_file, config_data):
    """Alter Kafka topic."""
    if not topics:
        raise click.UsageError("At least one topic must be specified with --topic")
        
    if config_file:
        config_data = json.load(config_file)
    elif config_data:
        config_data = json.loads(config_data)
    else:
        config_data = {}

    admin_client = ctx.get("admin_client")
    topic = Topic(admin_client)
    click.echo(json.dumps(topic.alter(topics, config_data)))