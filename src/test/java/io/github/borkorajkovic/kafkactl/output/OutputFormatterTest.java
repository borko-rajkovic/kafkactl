package io.github.borkorajkovic.kafkactl.output;

import io.github.borkorajkovic.kafkactl.kafka.OffsetChange;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutputFormatterTest {

    private static final List<OffsetChange> CHANGES = List.of(
            new OffsetChange("orders", 0, 100, null, 100),
            new OffsetChange("orders", 1, 200, 50L, 200)
    );

    @Test
    void textFormatReportsNoMatchesWhenEmpty() {
        String result = OutputFormatter.format(List.of(), OutputFormatter.Format.TEXT, false);
        assertEquals("No matching partitions found.", result);
    }

    @Test
    void textFormatListsEachChangeAndSummary() {
        String result = OutputFormatter.format(CHANGES, OutputFormatter.Format.TEXT, false);
        assertTrue(result.contains("orders"));
        assertTrue(result.contains("Planned (dry-run) 2 offset change(s)."));
    }

    @Test
    void textFormatMarksExecutedRun() {
        String result = OutputFormatter.format(CHANGES, OutputFormatter.Format.TEXT, true);
        assertTrue(result.contains("Applied 2 offset change(s)."));
    }

    @Test
    void jsonFormatContainsExpectedFields() {
        String result = OutputFormatter.format(CHANGES, OutputFormatter.Format.JSON, true);
        assertTrue(result.contains("\"executed\" : true"));
        assertTrue(result.contains("\"changeCount\" : 2"));
        assertTrue(result.contains("\"topic\" : \"orders\""));
    }
}
