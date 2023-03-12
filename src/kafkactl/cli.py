from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

from .get import get
from .create import create
from .describe import describe
from .alter import alter
from .delete import delete
from .exec import exec
from .produce import produce
from .consume import consume

import click
import json

class CatchAllExceptions(click.Group):
    """A click group that catches all exceptions and displays them as a message.
    This class extends the functionality of the `click.Group` class by adding
    a try-except block around the call to `cli()`. Any exceptions that are
    raised during the call to `cli()` are caught and displayed as a message
    to the user.
    """

    def __call__(self, *args, **kwargs):
        try:
            return self.main(*args, **kwargs)
        except KafkaException as e:
            click.echo(e)

@click.group("kafkactl", cls=CatchAllExceptions)
@click.version_option(package_name="kafkactl-py", prog_name="kafkactl")
@click.option("--bootstrap-servers", "-b", default=None, metavar="BROKERS", envvar="KAFKACTL_BOOTSTRAP_SERVERS", show_envvar=True, help="The Kafka bootstrap servers.")
@click.option("kafka_config", "--kafkaconfig", "-f", metavar="PATH", default=None, envvar="KAFKACONFIG", type=click.File("r"), help="Path to the configuration file in JSON format.")
@click.option("--log-level", "-l", type=click.Choice( ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"], case_sensitive=False, ), default="NOTSET", metavar="LEVEL", envvar="KAFKACTL_LOG_LEVEL", show_envvar=True, help="The logging level to use for the logger and console handler.")
@click.pass_context
def cli(ctx, bootstrap_servers, kafka_config, log_level):
    """A command-line client for Kafka."""

    if kafka_config:
        config = yaml.safe_load(kafka_config)
        current_ctx = config.get("current-context", None)

    if not bootstrap_servers:
        bootstrap_servers = ",".join(config.get(current_ctx, {}).get("brokers", []))

    ctx.obj = {
        "bootstrap_servers": bootstrap_servers,
        "admin_client": AdminClient({"bootstrap.servers": bootstrap_servers}),
        "log_level": log_level,
    }

cli.add_command(get)
cli.add_command(create)
cli.add_command(describe)
cli.add_command(alter)
cli.add_command(delete)
cli.add_command(exec)
cli.add_command(produce)
cli.add_command(consume)