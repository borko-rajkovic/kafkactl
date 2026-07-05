# kafkactl offset-copy — Implementation Plan

Status legend: `[ ]` pending · `[~]` in progress · `[x]` done

## Goal

Add a `copy-offsets` subcommand to `kafkactl` that copies/shifts/resets consumer
group offsets from one group to another, e.g.:

```
kafkactl copy-offsets --bootstrap-server localhost:9092 \
    --from-group old-group --to-group new-group --topics topic1,topic2

kafkactl copy-offsets --bootstrap-server localhost:9092 \
    --from-group old-group --to-group new-group --all-topics
```

## CLI options

| Option | Notes |
|---|---|
| `--bootstrap-server` | required |
| `--from-group` | required, source consumer group |
| `--to-group` | required, destination consumer group |
| `--topics topic1,topic2` | explicit topic list (mutually exclusive with `--all-topics`/`--regex`) |
| `--all-topics` | copy offsets for every topic the source group has committed |
| `--regex <pattern>` | select topics by regex (mutually exclusive with `--topics`/`--all-topics`) |
| `--exclude-topics topic1,topic2` | subtract from whichever selection above was used |
| `--shift +100` / `--shift -20` | shift copied offsets by N (clamped to [earliest, latest]) |
| `--reset-to-earliest` | ignore source offsets, reset target to each partition's earliest offset |
| `--reset-to-latest` | ignore source offsets, reset target to each partition's latest offset |
| `--dry-run` | default; print the plan, make no changes |
| `--execute` | actually commit the new offsets to `--to-group` |
| `--output text\|json` | default `text` |
| `--verbose` | extra logging |

`--shift`, `--reset-to-earliest`, `--reset-to-latest` are mutually exclusive; default
behavior with none of them is a straight copy.

## Architecture

- `Kafkactl` (root picocli command) — registers `CopyOffsetsCommand` as a subcommand.
- `command/CopyOffsetsCommand.java` — picocli `@Command`, owns all `@Option`s and
  orchestrates: connect → fetch source offsets → filter topics → calculate target
  offsets → render plan → (if `--execute`) alter offsets on `--to-group` with a
  progress bar.
- `kafka/TopicFilter.java` — pure logic: resolve the final topic set from
  available topics + `--topics`/`--all-topics`/`--regex`/`--exclude-topics`.
- `kafka/OffsetCalculator.java` — pure logic: given source offsets + mode
  (copy/shift/reset-earliest/reset-latest) + earliest/latest bounds, compute the
  target offset per partition (shift is clamped to valid range).
- `kafka/OffsetChange.java` — record describing one partition's planned change
  (topic, partition, source offset, current target offset if any, new offset).
- `kafka/OffsetService.java` — thin wrapper around `AdminClient`/`KafkaConsumer`
  for listing consumer group offsets, fetching earliest/latest offsets, and
  altering consumer group offsets. Not unit-tested directly (needs a live broker).
- `output/OutputFormatter.java` — pure logic: render a list of `OffsetChange` as
  a text table or JSON (via Jackson, already a transitive dependency).
- `output/ProgressBar.java` — simple console progress bar for `--execute`.

## Testing

JUnit 5 unit tests for the pure-logic pieces (no broker needed):

- `TopicFilterTest` — all-topics, explicit list, regex, exclude combinations,
  unknown topic handling.
- `OffsetCalculatorTest` — copy, shift positive/negative with clamping, reset to
  earliest/latest.
- `OutputFormatterTest` — text table and JSON rendering.
- `ProgressBarTest` — bar string rendering at various percentages.

## Docs

- `README.md` at repo root — usage, install/build, examples, option reference.

## Steps

- [x] Write this plan
- [x] Add JUnit 5 + surefire to `pom.xml`
- [x] `kafka/OffsetChange.java`
- [x] `kafka/TopicFilter.java` + test
- [x] `kafka/OffsetCalculator.java` + test
- [x] `output/OutputFormatter.java` + test
- [x] `output/ProgressBar.java` + test
- [x] `kafka/OffsetService.java` (AdminClient integration)
- [x] `command/CopyOffsetsCommand.java` + register on `Kafkactl`
- [x] `README.md`
- [x] `mvn test` passes (24/24), `mvn package` builds shaded jar; CLI help/validation smoke-tested manually

## Follow-up: cluster authentication (SASL/SSL)

- [x] `--command-config <file>` option — path to a Java properties file merged into the
      `AdminClient` config (`security.protocol`, `sasl.mechanism`, `sasl.jaas.config`,
      `ssl.*`, etc.), same convention as `kafka-consumer-groups.sh --command-config`.
      Keeps credentials out of the command line/shell history.
- [x] `kafka/ClientConfigLoader.java` — loads the properties file and merges it with
      `--bootstrap-server` (CLI flag always wins) + test.
- [x] `OffsetService` updated to take the merged `Properties` instead of a bare
      bootstrap-servers string.
- [x] Upfront validation: missing/unreadable `--command-config` file fails fast with a
      picocli `ParameterException`, before any network calls.
- [x] README section with SASL_SSL/PLAIN and mTLS example properties files.
- [x] `mvn test` passes (28/28), `mvn package` builds; `--command-config` error path
      smoke-tested manually.

## Notes

- `--shift` accepts signed integers via picocli/`Integer.valueOf` (`+100`, `-20`).
- Copy/shift results are clamped to each partition's `[earliest, latest]` range so a
  stale source offset or an aggressive shift can never produce an offset Kafka rejects.
- The progress bar writes to stderr, keeping `--output json` on stdout script-friendly.
- `OffsetService` (the `AdminClient` wrapper) is not unit tested — it has no branching
  logic of its own and would need a live/embedded broker to test meaningfully; it is
  exercised manually via the CLI.
