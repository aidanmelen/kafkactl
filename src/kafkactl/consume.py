from tabulate import tabulate
from kafka import (Cluster, Topic, ConsumerGroup, Acl, Consumer, Producer)

import click
import json

@click.group("consume")
@click.pass_obj
def consume(ctx):
    """Consume from one or many Kafka Topics."""
    pass
