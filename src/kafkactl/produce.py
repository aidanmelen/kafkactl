from tabulate import tabulate
from kafka import (Topic, Broker, Topic, ConsumerGroup, Acl, Consumer, Producer)

import click
import json

@click.group("produce")
@click.pass_obj
def produce(ctx):
    """Produce to a Kafka Topic."""
    pass
