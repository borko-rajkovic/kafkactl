# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

`kafkactl` is a CLI for managing Kafka consumer group offsets. The root command
(`io.github.borkorajkovic.kafkactl.Kafkactl`) currently has one subcommand,
`copy-offsets`, which copies/shifts/resets committed offsets from one consumer
group to another (dry-run by default). See `README.md` for full usage and
`docs/PLAN.md` for the implementation plan and history. Unit tests exist for
all pure-logic classes; the `AdminClient` integration is exercised manually
against a real cluster.

## Commands

Build (compiles and packages a shaded/uber jar via maven-shade-plugin):

```
mvn package
```

Run the CLI without building a jar:

```
mvn compile exec:java -Dexec.mainClass=io.github.borkorajkovic.kafkactl.Kafkactl -Dexec.args="--help"
```

Run the built jar directly:

```
java -jar target/kafkactl-1.0-SNAPSHOT.jar --help
java -jar target/kafkactl-1.0-SNAPSHOT.jar copy-offsets --help
```

Run tests:

```
mvn test
```

Run a single test class:

```
mvn test -Dtest=ClassNameTest
```

## Architecture

- **CLI framework**: [picocli](https://picocli.info/). The root command is `Kafkactl`
  (`@Command(name = "kafkactl", ...)`), executed via `new CommandLine(new Kafkactl()).execute(args)`
  in `main`. New CLI subcommands should be added as `@Command`-annotated classes implementing
  `Callable<Integer>` and registered in `Kafkactl`'s `@Command(subcommands = {...})`.
- **`copy-offsets` subcommand** (`command/CopyOffsetsCommand.java`): owns all `@Option`s/`@ArgGroup`s
  and orchestrates the flow — load client config → fetch source group offsets → resolve topic
  selection → compute target offsets → render the plan → (if `--execute`) apply with a progress bar.
  Business logic is deliberately kept out of this class and pushed into pure, unit-tested helpers:
  - `kafka/TopicFilter` — resolves the final topic set from `--all-topics`/`--topics`/`--regex`
    plus `--exclude-topics`.
  - `kafka/OffsetCalculator` — computes each partition's target offset for copy/shift/reset modes,
    clamped to `[earliest, latest]`.
  - `kafka/ClientConfigLoader` — merges an optional `--command-config` properties file (SASL/SSL
    auth, etc.) with `--bootstrap-server` into the `Properties` passed to `AdminClient`;
    `--bootstrap-server` always wins over anything in the file.
  - `output/OutputFormatter` — renders the plan as text or JSON.
  - `output/ProgressBar` — console progress bar for `--execute`, written to stderr so it never
    pollutes `--output json` on stdout.
  - `kafka/OffsetService` — the only class that touches Kafka's `AdminClient` directly (list/alter
    consumer group offsets, fetch earliest/latest offsets). Not unit tested; requires a live broker.
- **Kafka access**: `kafka-clients` `AdminClient`, wrapped by `OffsetService`.
- **Auth**: cluster credentials are never passed on the command line. `--command-config <file>`
  points at a Java properties file (same convention as `kafka-consumer-groups.sh --command-config`)
  containing `security.protocol`, `sasl.mechanism`, `sasl.jaas.config`, `ssl.*`, etc. See the
  README's "Authenticating to a secured cluster" section for examples.
- **Logging**: SLF4J API with a Log4j2 backend (`log4j-slf4j2-impl` + `log4j-core`). Logging config
  lives in `src/main/resources/log4j2.yaml` (YAML-based Log4j2 config, requires the
  `jackson-dataformat-yaml` dependency also present in the POM). Get loggers via
  `LoggerFactory.getLogger(...)`, not the Log4j2 API directly. `--verbose` additionally prints
  progress details to stderr independent of the log4j2 level.
- **JSON**: rendered via Jackson's `ObjectMapper` (`jackson-databind`, pulled in transitively by
  `jackson-dataformat-yaml` — no separate JSON dependency needed).
- **Testing**: JUnit 5 (`junit-jupiter`), run via `maven-surefire-plugin`. Tests live under
  `src/test/java`, mirroring the main package structure.
- **Packaging**: `maven-shade-plugin` builds a single executable uber-jar on `mvn package`, with
  `Kafkactl` set as the manifest main class. The shade config merges `META-INF/services` files
  (`ServicesResourceTransformer`) and appends Log4j2's plugin descriptor
  (`Log4j2Plugins.dat`) across dependencies — required for Log4j2 plugins to be discovered
  correctly from a shaded jar.
- **Java/Maven baseline**: Java 21 (`maven.compiler.source/target`), Maven multi-module is not
  in use (single module).
