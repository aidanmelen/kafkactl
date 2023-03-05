from .topic import Topic

import click
import json

@click.group("list")
@click.pass_obj
def list(ctx):
    """List one or many resources."""
    pass

@list.command("topics")
@click.pass_obj
def list_topics(ctx):
    """List Kafka topics."""
    topic = Topic(ctx.get("admin_client"))
    click.echo(json.dumps(topic.list()))