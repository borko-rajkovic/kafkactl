package io.github.borkorajkovic.kafkactl.output;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.borkorajkovic.kafkactl.kafka.OffsetChange;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Renders a list of {@link OffsetChange} as either a human-readable text
 * table or JSON.
 */
public final class OutputFormatter {

    public enum Format {
        TEXT,
        JSON
    }

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private OutputFormatter() {
    }

    public static String format(List<OffsetChange> changes, Format format, boolean executed) {
        return switch (format) {
            case JSON -> toJson(changes, executed);
            case TEXT -> toText(changes, executed);
        };
    }

    private static String toText(List<OffsetChange> changes, boolean executed) {
        if (changes.isEmpty()) {
            return "No matching partitions found.";
        }

        StringBuilder sb = new StringBuilder();
        String header = String.format(
                "%-30s %-10s %-15s %-15s %-15s%n", "TOPIC", "PARTITION", "SOURCE", "TARGET (before)", "TARGET (after)");
        sb.append(header);
        sb.append("-".repeat(header.length() - 1)).append(System.lineSeparator());

        for (OffsetChange change : changes) {
            sb.append(String.format(
                    "%-30s %-10d %-15d %-15s %-15d%n",
                    change.topic(),
                    change.partition(),
                    change.sourceOffset(),
                    change.currentTargetOffset() == null ? "-" : change.currentTargetOffset(),
                    change.newOffset()));
        }

        sb.append(executed ? "Applied " : "Planned (dry-run) ")
                .append(changes.size())
                .append(" offset change(s).");

        return sb.toString();
    }

    private static String toJson(List<OffsetChange> changes, boolean executed) {
        try {
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("executed", executed);
            root.put("changeCount", changes.size());
            root.put("changes", changes);
            return JSON_MAPPER.writeValueAsString(root);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to render JSON output", e);
        }
    }
}
