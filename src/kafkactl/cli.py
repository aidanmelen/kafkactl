from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

from .list import list
from .create import create
from .describe import describe
from .alter import alter
from .delete import delete
from .exec import exec
from .produce import produce
from .consume import consume

import click
import json
import os
import traceback

class CatchAllExceptions(click.Group):
    """A click group that catches all exceptions and displays them as a message.
    This class extends the functionality of the `click.Group` class by adding
    a try-except block around the call to `main()`. Any exceptions that are
    raised during the call to `main()` are caught and displayed as a message
    to the user.
    """

    def __call__(self, *args, **kwargs):
        try:
            return self.main(*args, **kwargs)
        except KafkaException as e:
            error_msg = {"error_message": str(e)}
            click.echo(json.dumps(error_msg))
        except Exception as e:
            if os.environ.get("KAFKACTL_ENABLE_TRACEBACK", "false").lower() == "true":
                traceback.print_exc()
                error_msg = {"error_message": str(e)}
                click.echo(json.dumps(error_msg))
            else:
                error_msg = " ".join(
                    [
                        f"Oops! An unknown error has occurred: '{e}'.",
                        "Setting KAFKACTL_ENABLE_TRACEBACK=true will provide more information in the event of a python error.",
                        "Please see consider opening an issue: https://github.com/aidanmelen/kafka-connect-py/issues",
                    ]
                )
                click.echo(json.dumps({"error_message": error_msg}))

@click.group("kafkactl", cls=CatchAllExceptions)
@click.version_option(package_name="kafkactl-py", prog_name="kafka|kafkactl")
@click.option("--bootstrap-servers", "-b", default="localhost:9092", metavar="BROKERS", envvar="KAFKACTL_BOOTSTRAP_SERVERS", show_envvar=True, help="The Kafka bootstrap servers.")
@click.option("--client-id", "-id", default="kafkactl", metavar="ID", envvar="KAFKACTL_CLIENT_ID", show_envvar=True, help="The Kafka client ID.")
@click.option("--log-level", "-l", type=click.Choice( ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"], case_sensitive=False, ), default="NOTSET", metavar="LEVEL", envvar="KAFKACTL_LOG_LEVEL", show_envvar=True, help="The logging level to use for the logger and console handler.")
@click.pass_context
def cli(ctx, bootstrap_servers, client_id, log_level):
    """A command-line client for Kafka."""
    ctx.obj = {
        "admin_client": AdminClient({"bootstrap.servers": bootstrap_servers}),
        "log_level": log_level,
    }

cli.add_command(list)
cli.add_command(create)
cli.add_command(describe)
cli.add_command(alter)
cli.add_command(delete)
cli.add_command(exec)
cli.add_command(produce)
cli.add_command(consume)