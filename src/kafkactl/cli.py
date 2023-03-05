from confluent_kafka.admin import AdminClient

from .helpers import get_logger

from .list import list
from .create import create
from .describe import describe
from .alter import alter
from .delete import delete

import click


@click.group("kafka")
@click.version_option(package_name="kafkactl-py", prog_name="kafka|kafkactl")
@click.option("--bootstrap-servers", "-b", default="localhost:9092", metavar="BROKERS", envvar="KAFKA_BOOTSTRAP_SERVERS", show_envvar=True, help="The Kafka bootstrap servers.")
@click.option("--client-id", "-id", default="kafkactl", metavar="ID", envvar="kafkactl_ID", show_envvar=True, help="The Kafka client ID.")
@click.option("--log-level", "-l", type=click.Choice( ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"], case_sensitive=False, ), default="NOTSET", metavar="LEVEL", envvar="kafkactl_LOG_LEVEL", show_envvar=True, help="The logging level to use for the logger and console handler.")
@click.pass_context
def cli(ctx, bootstrap_servers, client_id, log_level):
    """A command-line client for Kafka."""
    ctx.obj = {
        "admin_client": AdminClient({"bootstrap.servers": bootstrap_servers}),
        "logger": get_logger(log_level),
    }

cli.add_command(list)
cli.add_command(create)
cli.add_command(describe)
cli.add_command(alter)
cli.add_command(delete)