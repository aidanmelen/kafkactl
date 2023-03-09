<!-- [![PyPI](https://img.shields.io/pypi/v/kafkactl_py.svg)](https://pypi.org/project/kafkactl-py/)
[![Dockerhub](https://img.shields.io/docker/v/aidanmelen/kafkactl-py?color=blue&label=docker%20build)](https://hub.docker.com/r/aidanmelen/kafkactl-py)
[![Tests](https://github.com/aidanmelen/kafkactl-py/actions/workflows/tests.yaml/badge.svg)](https://github.com/aidanmelen/kafkactl-py/actions/workflows/tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/aidanmelen/kafkactl-py/badge.svg?branch=main)](https://coveralls.io/github/aidanmelen/kafkactl-py?branch=main) -->

# Kafkactl

A command line tool for controlling and interacting with Kafka.
## Install

```bash
pip install kafkactl
```

## Examples

### Client

`kafkactl` supports multiple options for specifying the kafka connection information. Here are the options in order of precedence:

1. The command line option: `kafkactl --bootstrap-servers kafka:9092 <sub-command>`
2. The environment variable: `KAFKACTL_BOOTSTRAP_SERVERS=kafka:9092 kafkactl <sub-command>`
3. The config file: `kafkactl --config-file ./kafkaconfig.config <sub-command>`

### Brokers

List the Kafka brokers information.

```console
$ kafkactl list brokers
  BROKER  TYPE        ENDPOINT
       0  Controller  kafka-0.kafka.confluent.svc.cluster.local:9092
       2  Worker      kafka-2.kafka.confluent.svc.cluster.local:9092
       1  Worker      kafka-1.kafka.confluent.svc.cluster.local:9092
```

Get the Kafka cluster overview.

```console
$ kafkactl describe brokers
  BROKERS    TOPICS    PARTITIONS    REPLICAS    CONSUMER_GROUPS
        3        60           749        2245                  3
```

Get the default cluster configuration. We are using `head` to limit the output to the first 10 default properties.

```console
$ kafkactl get cluster-defaults | head -n 11
PROPERTY-NAME                                      PROPERTY-VALUE
sasl.oauthbearer.jwks.endpoint.refresh.ms          3600000
remote.log.metadata.manager.listener.name          -
controller.socket.timeout.ms                       30000
log.flush.interval.ms                              -
controller.quorum.request.timeout.ms               2000
min.insync.replicas                                2
confluent.tier.bucket.probe.period.ms              -1
remote.log.manager.thread.pool.size                10
confluent.tier.metadata.request.timeout.ms         30000
```

Note that all commands support the `--output/-o json` to format the output as JSON. For example,

```console
$ kafkactl get cluster-defaults --output json | jq
{
  "sasl.oauthbearer.jwks.endpoint.refresh.ms": "3600000",
  "remote.log.metadata.manager.listener.name": null,
  "controller.socket.timeout.ms": "30000",
  ...
  "confluent.balancer.max.replicas": "2147483647",
  "queued.max.request.bytes": "-1",
  "confluent.max.connection.creation.rate.per.ip": "2147483647"
}
```

### Topics

Create a Kafka Topic.

```console
$ kafkactl create topic topic1
```

List the Kafka Topic and hide the internal topics.

```console
$ kafkactl list topics --hide-internal
TOPIC                                PARTITION
confluent.connect-offsets                   25
confluent.connect-configs                    1
_schemas_schemaregistry_confluent            1
confluent.connect-status                     5
```

and describe the Kafka Topic we just created.

```
$ kafkactl describe topics --topic topic1
TOPIC    STATUS      PARTITION    LEADER  REPLICAS    IN-SYNC-REPLICAS
topic1   Healthy             0         0  [0, 1, 2]   [0, 1, 2]
topic1   Healthy             1         1  [1, 2, 0]   [1, 2, 0]
topic1   Healthy             2         2  [2, 0, 1]   [2, 0, 1]
```

Or describe all Kafka Topics.

```console
$ kafkactl describe topics
...
```

Alter configuration atomically for a Kafka Topic, replacing non-specified configuration properties with the cluster default values.

```console
$ kafkactl alter topic topic1 -d '{"cleanup.policy": "compact"}'
```

And verify the alteration by getting the configuration information for the Kafka Topic. We are filtering out confluent specific configurations using `grep` for brevity.

```console
$ kafkactl get topic-configs -t topic1 | grep -v "confluent"
TOPIC    PROPERTY-NAME                                     PROPERTY-VALUE
topic1   compression.type                                  producer
topic1   leader.replication.throttled.replicas             -
topic1   message.downconversion.enable                     true
topic1   min.insync.replicas                               2
topic1   segment.jitter.ms                                 0
topic1   cleanup.policy                                    compact
topic1   flush.ms                                          9223372036854775807
topic1   follower.replication.throttled.replicas           -
topic1   segment.bytes                                     1073741824
topic1   retention.ms                                      604800000
topic1   flush.messages                                    9223372036854775807
topic1   message.format.version                            2.6-IV0
topic1   max.compaction.lag.ms                             9223372036854775807
topic1   file.delete.delay.ms                              60000
topic1   max.message.bytes                                 1048588
topic1   min.compaction.lag.ms                             0
topic1   message.timestamp.type                            CreateTime
topic1   preallocate                                       false
topic1   min.cleanable.dirty.ratio                         0.5
topic1   index.interval.bytes                              4096
topic1   unclean.leader.election.enable                    false
topic1   retention.bytes                                   -1
topic1   delete.retention.ms                               86400000
topic1   segment.ms                                        604800000
topic1   message.timestamp.difference.max.ms               9223372036854775807
topic1   segment.index.bytes                               10485760
```

Delete a Kafka Topic.

```console
kafkactl delete topic topic1
```

### Consumer Groups

>**Note**: Create a consumer group by starting a high-level consumer.

List Consumer Group information.

```
kafkactl list groups
GROUP                                                   TYPE        STATE
ConfluentTelemetryReporterSampler--4883329356937767580  High-level  Stable
_confluent-controlcenter-7-3-0-0-command                High-level  Stable
_confluent-controlcenter-7-3-0-0                        High-level  Stable
```

Describe a single Kafka Consumer Group.

```console
kafkactl describe groups --group group1
GROUP   TOPIC  PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG     CONSUMER-ID
group1  topic1 0          1               3               2       test-consumer-group.local-1456198719410-29ccd54f-0
```

Or describe all Kafka Consumer Groups.

```console
$ kafkactl describe groups
...
```

Delete a Consumer Group.

```console
kafkactl delete group group1
```





## License

[Apache 2.0 License - aidanmelen/kafkactl](https://github.com/aidanmelen/kafkactl/blob/main/README.md)

## Credits

This project was influenced by [confluent-kafka-python/examples/adminapi.py](https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/adminapi.py).
