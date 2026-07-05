# kafkactl

A CLI utility for managing Kafka consumer group offsets — copy, shift, or
reset offsets from one consumer group to another, with a dry-run preview by
default.

## Build

```
mvn package
```

Produces a runnable uber-jar at `target/kafkactl-1.0-SNAPSHOT.jar`.

## Run

```
java -jar target/kafkactl-1.0-SNAPSHOT.jar --help
```

Or during development, without building a jar:

```
mvn compile exec:java -Dexec.mainClass=io.github.borkorajkovic.kafkactl.Kafkactl -Dexec.args="--help"
```

## `copy-offsets`

Copies committed offsets from `--from-group` to `--to-group` for a selected
set of topics. By default it only prints the planned changes (dry-run); pass
`--execute` to actually commit them.

### Basic copy

```
kafkactl copy-offsets \
    --bootstrap-server localhost:9092 \
    --from-group old-group \
    --to-group new-group \
    --topics topic1,topic2 \
    --execute
```

### All topics

```
kafkactl copy-offsets \
    --bootstrap-server localhost:9092 \
    --from-group old-group \
    --to-group new-group \
    --all-topics
```

### Topic selection by regex, with exclusions

```
kafkactl copy-offsets \
    --bootstrap-server localhost:9092 \
    --from-group old-group --to-group new-group \
    --regex 'orders-.*' \
    --exclude-topics orders-dlq
```

### Shifting offsets

Move every copied offset forward or back by a fixed amount (clamped to each
partition's valid `[earliest, latest]` range):

```
kafkactl copy-offsets --bootstrap-server localhost:9092 \
    --from-group old-group --to-group new-group --all-topics \
    --shift +100 --execute

kafkactl copy-offsets --bootstrap-server localhost:9092 \
    --from-group old-group --to-group new-group --all-topics \
    --shift -20 --execute
```

### Resetting instead of copying

```
kafkactl copy-offsets --bootstrap-server localhost:9092 \
    --from-group old-group --to-group new-group --all-topics \
    --reset-to-earliest --execute

kafkactl copy-offsets --bootstrap-server localhost:9092 \
    --from-group old-group --to-group new-group --all-topics \
    --reset-to-latest --execute
```

### JSON output

```
kafkactl copy-offsets --bootstrap-server localhost:9092 \
    --from-group old-group --to-group new-group --all-topics \
    --output json
```

### Authenticating to a secured cluster (SASL/SSL)

Credentials never go on the command line. Instead, pass a Java properties
file via `--command-config` — the same convention as Kafka's own
`kafka-consumer-groups.sh --command-config`. Its contents are merged into the
`AdminClient` config; `--bootstrap-server` always wins if the file also sets
`bootstrap.servers`.

`client.properties` for SASL_SSL/PLAIN:

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="my-user" password="my-password";
```

`client.properties` for mTLS (SSL, no SASL):

```properties
security.protocol=SSL
ssl.truststore.location=/path/to/truststore.jks
ssl.truststore.password=changeit
ssl.keystore.location=/path/to/keystore.jks
ssl.keystore.password=changeit
```

```
kafkactl copy-offsets \
    --bootstrap-server broker1:9093,broker2:9093 \
    --command-config /secure/path/client.properties \
    --from-group old-group --to-group new-group --all-topics \
    --execute
```

Keep this file outside version control and readable only by the user running
`kafkactl` (e.g. `chmod 600 client.properties`).

## Options

| Option | Description |
|---|---|
| `--bootstrap-server <host:port>` | Required. Kafka bootstrap server(s). |
| `--command-config <file>` | Path to a Java properties file with additional client config (auth type + credentials, e.g. `security.protocol`, `sasl.mechanism`, `sasl.jaas.config` for SASL, or `ssl.*` for mTLS). |
| `--from-group <group>` | Required. Source consumer group. |
| `--to-group <group>` | Required. Destination consumer group. |
| `--topics topic1,topic2` | Explicit topic list. Mutually exclusive with `--all-topics`/`--regex`. |
| `--all-topics` | Select every topic with committed offsets in `--from-group`. |
| `--regex <pattern>` | Select topics whose name matches the regex. |
| `--exclude-topics topic1,topic2` | Subtract these topics from whichever selection above was used. |
| `--shift +100` / `--shift -20` | Shift each copied offset by N, clamped to `[earliest, latest]`. |
| `--reset-to-earliest` | Ignore source offsets; reset the target group to each partition's earliest offset. |
| `--reset-to-latest` | Ignore source offsets; reset the target group to each partition's latest offset. |
| `--dry-run` | Print the plan only. This is the default when neither flag is given. |
| `--execute` | Apply the planned changes to `--to-group`. Mutually exclusive with `--dry-run`. |
| `--output text\|json` | Output format. Defaults to `text`. |
| `-v`, `--verbose` | Print extra progress details to stderr. |

Exactly one of `--all-topics`, `--topics`, or `--regex` is required. At most
one of `--shift`, `--reset-to-earliest`, `--reset-to-latest` may be given; if
none are given, offsets are copied as-is.

A progress bar is shown on stderr while `--execute` applies changes, so it
doesn't interfere with `--output json` piped from stdout.

## Development

Run tests:

```
mvn test
```

Run a single test class:

```
mvn test -Dtest=OffsetCalculatorTest
```

The offset-selection and offset-calculation logic (`TopicFilter`,
`OffsetCalculator`, `OutputFormatter`, `ProgressBar`, `ClientConfigLoader`) is
unit tested without needing a live Kafka broker. The Kafka `AdminClient`
integration (`OffsetService`) is exercised through the CLI against a real
cluster.
