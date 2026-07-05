package io.github.borkorajkovic.kafkactl.command;

import io.github.borkorajkovic.kafkactl.kafka.ClientConfigLoader;
import io.github.borkorajkovic.kafkactl.kafka.OffsetCalculator;
import io.github.borkorajkovic.kafkactl.kafka.OffsetChange;
import io.github.borkorajkovic.kafkactl.kafka.OffsetService;
import io.github.borkorajkovic.kafkactl.kafka.TopicFilter;
import io.github.borkorajkovic.kafkactl.output.OutputFormatter;
import io.github.borkorajkovic.kafkactl.output.ProgressBar;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@Command(
        name = "copy-offsets",
        mixinStandardHelpOptions = true,
        description = "Copy, shift, or reset consumer group offsets from one group to another"
)
public class CopyOffsetsCommand implements Callable<Integer> {

    private static final Logger log = LoggerFactory.getLogger(CopyOffsetsCommand.class);

    @Spec
    private CommandSpec spec;

    @Option(names = "--bootstrap-server", required = true, description = "Kafka bootstrap server(s)")
    private String bootstrapServer;

    @Option(
            names = "--command-config",
            description = "Path to a Java properties file with additional client config, "
                    + "e.g. security.protocol=SASL_SSL, sasl.mechanism, sasl.jaas.config for SASL/SSL auth"
    )
    private Path commandConfig;

    @Option(names = "--from-group", required = true, description = "Source consumer group")
    private String fromGroup;

    @Option(names = "--to-group", required = true, description = "Destination consumer group")
    private String toGroup;

    @ArgGroup(multiplicity = "1")
    private TopicSelection topicSelection;

    @Option(names = "--exclude-topics", split = ",", description = "Topics to exclude from the selection")
    private List<String> excludeTopics;

    @ArgGroup(multiplicity = "0..1")
    private ResetMode resetMode;

    @Option(names = "--dry-run", description = "Print the planned changes without applying them (default)")
    private boolean dryRun;

    @Option(names = "--execute", description = "Apply the planned changes to --to-group")
    private boolean execute;

    @Option(names = "--output", defaultValue = "text", description = "Output format: text or json")
    private String outputFormat;

    @Option(names = {"-v", "--verbose"}, description = "Print extra progress details")
    private boolean verbose;

    static class TopicSelection {
        @Option(names = "--all-topics", required = true, description = "Select every topic with committed offsets in --from-group")
        boolean allTopics;

        @Option(names = "--topics", required = true, split = ",", description = "Comma-separated list of topics")
        List<String> topics;

        @Option(names = "--regex", required = true, description = "Regex matched against available topic names")
        String regex;
    }

    static class ResetMode {
        @Option(names = "--shift", description = "Shift copied offsets by N, e.g. +100 or -20")
        Integer shift;

        @Option(names = "--reset-to-earliest", description = "Reset target offsets to each partition's earliest offset")
        boolean resetToEarliest;

        @Option(names = "--reset-to-latest", description = "Reset target offsets to each partition's latest offset")
        boolean resetToLatest;
    }

    @Override
    public Integer call() {
        validateOptions();

        OutputFormatter.Format format = parseOutputFormat();
        boolean willExecute = execute;

        try {
            Properties clientConfig = ClientConfigLoader.load(bootstrapServer, commandConfig);
            return run(clientConfig, format, willExecute);
        } catch (Exception e) {
            log.error("copy-offsets failed", e);
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private Integer run(Properties clientConfig, OutputFormatter.Format format, boolean willExecute) throws Exception {
        try (OffsetService offsetService = new OffsetService(clientConfig)) {
            verbose("Fetching offsets for source group '" + fromGroup + "'...");
            Map<TopicPartition, Long> sourceOffsets = offsetService.fetchGroupOffsets(fromGroup);

            Set<String> availableTopics = sourceOffsets.keySet().stream()
                    .map(TopicPartition::topic)
                    .collect(Collectors.toSet());

            Set<String> selectedTopics = TopicFilter.resolve(
                    availableTopics,
                    topicSelection.allTopics,
                    topicSelection.topics,
                    topicSelection.regex,
                    excludeTopics
            );

            Map<TopicPartition, Long> filteredSource = sourceOffsets.entrySet().stream()
                    .filter(entry -> selectedTopics.contains(entry.getKey().topic()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            List<OffsetChange> changes = buildPlannedChanges(offsetService, filteredSource);

            System.out.println(OutputFormatter.format(changes, format, willExecute));

            if (willExecute && !changes.isEmpty()) {
                applyChanges(offsetService, changes);
            }

            return 0;
        }
    }

    private List<OffsetChange> buildPlannedChanges(
            OffsetService offsetService, Map<TopicPartition, Long> filteredSource
    ) throws Exception {
        if (filteredSource.isEmpty()) {
            return List.of();
        }

        OffsetCalculator.Mode mode = resolveMode();
        int shiftAmount = (resetMode != null && resetMode.shift != null) ? resetMode.shift : 0;

        verbose("Fetching earliest/latest offsets for " + filteredSource.size() + " partition(s)...");
        Map<TopicPartition, Long> earliest = offsetService.fetchEarliestOffsets(filteredSource.keySet());
        Map<TopicPartition, Long> latest = offsetService.fetchLatestOffsets(filteredSource.keySet());

        verbose("Fetching offsets for destination group '" + toGroup + "'...");
        Map<TopicPartition, Long> targetOffsets = offsetService.fetchGroupOffsets(toGroup);

        List<TopicPartition> sortedPartitions = filteredSource.keySet().stream()
                .sorted(Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition))
                .toList();

        List<OffsetChange> changes = new ArrayList<>();
        for (TopicPartition partition : sortedPartitions) {
            long sourceOffset = filteredSource.get(partition);
            long earliestOffset = earliest.getOrDefault(partition, 0L);
            long latestOffset = latest.getOrDefault(partition, sourceOffset);
            long newOffset = OffsetCalculator.calculate(mode, sourceOffset, shiftAmount, earliestOffset, latestOffset);

            changes.add(new OffsetChange(
                    partition.topic(),
                    partition.partition(),
                    sourceOffset,
                    targetOffsets.get(partition),
                    newOffset
            ));
        }
        return changes;
    }

    private void applyChanges(OffsetService offsetService, List<OffsetChange> changes) throws Exception {
        ProgressBar progressBar = new ProgressBar(changes.size());
        int completed = 0;
        progressBar.update(completed);

        for (OffsetChange change : changes) {
            TopicPartition partition = new TopicPartition(change.topic(), change.partition());
            offsetService.alterGroupOffsets(toGroup, Map.of(partition, change.newOffset()));
            completed++;
            progressBar.update(completed);
        }

        progressBar.complete();
    }

    private OffsetCalculator.Mode resolveMode() {
        if (resetMode == null) {
            return OffsetCalculator.Mode.COPY;
        }
        if (resetMode.shift != null) {
            return OffsetCalculator.Mode.SHIFT;
        }
        if (resetMode.resetToEarliest) {
            return OffsetCalculator.Mode.RESET_EARLIEST;
        }
        if (resetMode.resetToLatest) {
            return OffsetCalculator.Mode.RESET_LATEST;
        }
        return OffsetCalculator.Mode.COPY;
    }

    private OutputFormatter.Format parseOutputFormat() {
        try {
            return OutputFormatter.Format.valueOf(outputFormat.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new ParameterException(spec.commandLine(), "Invalid --output value '" + outputFormat + "', expected 'text' or 'json'");
        }
    }

    private void validateOptions() {
        if (dryRun && execute) {
            throw new ParameterException(spec.commandLine(), "--dry-run and --execute are mutually exclusive");
        }
        if (commandConfig != null && (!Files.isRegularFile(commandConfig) || !Files.isReadable(commandConfig))) {
            throw new ParameterException(spec.commandLine(), "--command-config file not readable: " + commandConfig);
        }
    }

    private void verbose(String message) {
        if (verbose) {
            System.err.println("[verbose] " + message);
        }
        log.debug(message);
    }
}
