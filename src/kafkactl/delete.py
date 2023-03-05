from .topic import Topic

import click
import json

@click.group("delete")
@click.pass_obj
def delete(ctx):
    """Delete one or many resources."""
    pass

@delete.command("topics")
@click.option("topics", "--topic", "-t", multiple=True, metavar="TOPIC", help="The name of the Kafka topic. This option can be used multiple times to specify multiple topics.")
@click.option("--timeout", "-to", default=30, metavar="SECONDS", type=int, help="The timeout in seconds.")
@click.pass_obj
def delete_topics(ctx, topics, timeout):
    """Delete Kafka topics."""
    topic = Topic(ctx.get("admin_client"))
    click.echo(json.dumps(topic.delete(topics, timeout)))