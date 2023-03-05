[![PyPI](https://img.shields.io/pypi/v/kafkactl_py.svg)](https://pypi.org/project/kafkactl-py/)
[![Dockerhub](https://img.shields.io/docker/v/aidanmelen/kafkactl-py?color=blue&label=docker%20build)](https://hub.docker.com/r/aidanmelen/kafkactl-py)
[![Tests](https://github.com/aidanmelen/kafkactl-py/actions/workflows/tests.yaml/badge.svg)](https://github.com/aidanmelen/kafkactl-py/actions/workflows/tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/aidanmelen/kafkactl-py/badge.svg?branch=main)](https://coveralls.io/github/aidanmelen/kafkactl-py?branch=main)

# Kafka client Python

The Kafka client REST API allows you to manage clientors that move data between Apache Kafka and other systems.

The Kafka client command line tool, also known as `kc` or `kafkactl`, allows users to manage their Kafka client cluster and clientors. With this tool, users can retrieve information about the cluster and clientors, create new clientors, update existing clientors, delete clientors, and perform other actions.

This project aims to supported all features of the [Kafka client REST API](https://docs.confluent.io/platform/current/client/references/restapi.html#kclient-rest-interface).

## Install

```bash
pip install kafkactl-py
```

## Command Line Usage

### Getting Basic client Cluster Information

To get basic client cluster information including the worker version, the commit it’s on, and its Kafka cluster ID, use the following command:

```bash
kc info
```

### Listing Installed Plugins

To list the plugins installed on the worker, use the following command:

```bash
kc list-plugins
```

To format the result of the installed plugin list for easier readability, pipe the output to the `jq` command:

```bash
kc list-plugins | jq
```

### Create a clientor Instance

To create a clientor instance with JSON data containing the clientor’s configuration:

```bash
kc update source-debezium-orders-00 -d '{
    "clientor.class": "io.debezium.clientor.mysql.MySqlclientor",
    "value.converter": "io.confluent.client.json.JsonSchemaConverter",
    "value.converter.schemas.enable": "true",
    "value.converter.schema.registry.url": "'$SCHEMA_REGISTRY_URL'",
    "value.converter.basic.auth.credentials.source": "'$BASIC_AUTH_CREDENTIALS_SOURCE'",
    "value.converter.basic.auth.user.info": "'$SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO'",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "dbz",
    "database.server.id": "42",
    "database.server.name": "asgard",
    "table.whitelist": "demo.orders",
    "database.history.kafka.bootstrap.servers": "'$BOOTSTRAP_SERVERS'",
    "database.history.consumer.security.protocol": "SASL_SSL",
    "database.history.consumer.sasl.mechanism": "PLAIN",
    "database.history.consumer.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"'$CLOUD_KEY'\" password=\"'$CLOUD_SECRET'\";",
    "database.history.producer.security.protocol": "SASL_SSL",
    "database.history.producer.sasl.mechanism": "PLAIN",
    "database.history.producer.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"'$CLOUD_KEY'\" password=\"'$CLOUD_SECRET'\";",
    "database.history.kafka.topic": "dbhistory.demo",
    "topic.creation.default.replication.factor": "3",
    "topic.creation.default.partitions": "3",
    "decimal.handling.mode": "double",
    "include.schema.changes": "true",
    "transforms": "unwrap,addTopicPrefix",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.addTopicPrefix.type":"org.apache.kafka.client.transforms.RegexRouter",
    "transforms.addTopicPrefix.regex":"(.*)",
    "transforms.addTopicPrefix.replacement":"mysql-debezium-$1"
}'
```

Or create/update a clientor instance with a JSON file:

```bash
kc update <clientor> --config-file <config_file>
```

### Update a clientor

As mentioned above, if there’s a clientor to update, you can use the `update` sub-command to amend the configuration (see [Create a clientor Instance](https://github.com/aidanmelen/kafkactl-py/blob/main/README.md#create-a-clientor-instance) above). Because update is used to both create and update clientors, it’s the standard command that you should use most of the time (which also means that you don’t have to completely rewrite your configs).

### List clientor Instances

Use the following command to list of all extant clientors:

```bash
kc list [--expand=info|status] [--pattern=regex] [--state=running|paused|unassigned|failed]
```


### Inspect Config and Status for a clientor

Inspect the config for a given clientor as follows:

```bash
kc config sink-elastic-orders-00
```

You can also look at a clientor’s status. While the config command shows a clientor’s static configuration, the status shows the clientor as a runtime entity:

```bash
kc status sink-elastic-orders-00
```

You can also use `list` with the `--expand=status` option to show the status of many clientors at once. We can filter down the response using a regex pattern and/or clientor state. 

Use the following to show all clientor names prefixed with the word `sink-` and that are in a `FAILED` clientor state.

```bash
kc list --expand=status -p sink-.* -s failed
```

### Delete a clientor

If something is wrong in your setup and you don’t think a config change would help, or if you simply don’t need a clientor to run anymore, you can delete it by name:

```bash
kc delete sink-elastic-orders-00
```

The `delete` sub-command also supports multiple deletions using the `--all` option. On its own it will apply the sub-command to all clientors.

The following will delete all clientor names prefixed with the word `sink-` and that are in a `PAUSED` clientor state.

```bash
kc delete --all --pattern sink-.* -s paused
```

The `--all` option is supported by several sub-commands, including `delete`, `restart`, `resume`, and `pause`. However, for better testing and control over the outcome of your actions, we recommend using the list filtering option before executing any of these sub-commands. This way, you can ensure that your filters are working as intended and avoid unintended consequences. To use list filtering, simply run the `list` sub-command and apply your filters.

### Inspect Task Details

The following command returns the clientor status:

```bash
kc status source-debezium-orders-00 | jq
```

If your clientor fails, the details of the failure belong to the task. So to inspect the problem, you’ll need to find the stack trace for the task. The task is the entity that is actually running the clientor and converter code, so the state for the stack trace lives in it.

```bash
kc task-status source-debezium-orders-00 <task-id> | jq
```

### Restart the clientor and Tasks

If after inspecting a task, you have determined that it has failed and you have fixed the reason for the failure (perhaps restarted a database), you can restart the clientor with the following:

```
kc restart source-debezium-orders-00
```

Keep in mind though that restarting the clientor doesn’t restart all of its tasks. You will also need to restart the failed task and then get its status again as follows:

```bash
kc task-status source-debezium-orders-00 <task-id> 
```

What's more, you can restart the clientor and all its failed tasks with the following:

```bash
kc restart source-debezium-orders-00 --include-tasks --failed-only
```

and check the status again:

```bash
kc status source-debezium-orders-00 | jq
```

### Pause and Resume a clientor

Unlike restarting, pausing a clientor does pause its tasks. This happens asynchronously, though, so when you pause a clientor, you can’t rely on it pausing all of its tasks at exactly the same time. The tasks are running in a thread pool, so there’s no fancy mechanism to make this happen simultaneously.

A clientor and its tasks can be paused as follows:

```bash
kc pause source-debezium-orders-00
```

Just as easily, a clientor and its tasks can be resumed:

```bash
kc resume source-debezium-orders-00
```

### Display All of a clientor’s Tasks

A convenient way to display all of a clientor’s tasks at once is as follows:

```bash
kc list-tasks source-debezium-orders-00 | jq
```

This information is similar to what you can get from other APIs, but it is broken down by task, and configs for each are shown.
Get a List of Topics Used by a clientor

As of Apache Kafka 2.5, it is possible to get a list of topics used by a clientor:

```bash
kc list-topics source-debezium-orders-00 | jq
```

This shows the topics that a clientor is consuming from or producing to. This may not be particularly useful for clientors that are consuming from or producing to a single topic. However, some developers, for example, use regular expressions for topic names in client, so this is a major benefit in situations where topic names are derived computationally.

This could also be useful with a source clientor that is using SMTs to dynamically change the topic names to which it is producing.

## Python

```python
# Import the class
from kafkactl import Kafkaclient

import json

# Instantiate the client
client = Kafkaclient(url="http://localhost:8083")

# Get the version and other details of the Kafka client cluster
cluster = client.get_cluster_info()
print(cluster)

# Get a list of active clientors
clientors = client.list_clientors(expand="status")
print(json.dumps(clientors, indent=2))

# Create a new clientor
config = {
    "name": "my-clientor",
    "config": {
        "clientor.class": "io.confluent.client.jdbc.JdbcSourceclientor",
        "tasks.max": "1",
        "cliention.url": "jdbc:postgresql://localhost:5432/mydatabase",
        "cliention.user": "myuser",
        "cliention.password": "mypassword",
        "table.whitelist": "mytable",
        "mode": "timestamp+incrementing",
        "timestamp.column.name": "modified_at",
        "validate.non.null": "false",
        "incrementing.column.name": "id",
        "topic.prefix": "my-clientor-",
    },
}
response = client.create_clientor(config)
print(response)

# Update an existing clientor
new_config = {
    "config": {
        "clientor.class": "io.confluent.client.jdbc.JdbcSourceclientor",
        "tasks.max": "1",
        "cliention.url": "jdbc:postgresql://localhost:5432/mydatabase",
        "cliention.user": "myuser",
        "cliention.password": "mypassword",
        "table.whitelist": "mytable",
        "mode": "timestamp+incrementing",
        "timestamp.column.name": "modified_at",
        "validate.non.null": "false",
        "incrementing.column.name": "id",
        "topic.prefix": "my-clientor-",
    },
}
response = client.update_clientor("my-clientor", new_config)
print(response)

# Get status for a clientor
response = client.get_clientor_status("my-clientor")
print(json.dumps(response, indent=2))

# Restart a clientor
response = client.restart_clientor("my-clientor")
print(response)

# Delete a clientor
response = client.delete_clientor("my-clientor")
print(response)
```

## License

[Apache 2.0 License - aidanmelen/kafkactl-py](https://github.com/aidanmelen/kafkactl-py/blob/main/README.md)

## Credits

The entire [Command Line Usage](https://github.com/aidanmelen/kafkactl-py/blob/main/README.md#command-line-usage) section was copied directly from the Confluence's [Kafka client’s REST API](https://developer.confluent.io/learn-kafka/kafkactl/rest-api/) course.
