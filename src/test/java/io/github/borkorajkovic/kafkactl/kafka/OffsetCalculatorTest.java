package io.github.borkorajkovic.kafkactl.kafka;

import org.junit.jupiter.api.Test;

import static io.github.borkorajkovic.kafkactl.kafka.OffsetCalculator.Mode.COPY;
import static io.github.borkorajkovic.kafkactl.kafka.OffsetCalculator.Mode.RESET_EARLIEST;
import static io.github.borkorajkovic.kafkactl.kafka.OffsetCalculator.Mode.RESET_LATEST;
import static io.github.borkorajkovic.kafkactl.kafka.OffsetCalculator.Mode.SHIFT;
import static org.junit.jupiter.api.Assertions.assertEquals;

class OffsetCalculatorTest {

    @Test
    void copyReturnsSourceOffsetWhenWithinBounds() {
        assertEquals(150, OffsetCalculator.calculate(COPY, 150, 0, 0, 1000));
    }

    @Test
    void copyClampsToLatestWhenSourceOffsetIsStale() {
        assertEquals(500, OffsetCalculator.calculate(COPY, 900, 0, 0, 500));
    }

    @Test
    void shiftPositiveAddsToSourceOffset() {
        assertEquals(250, OffsetCalculator.calculate(SHIFT, 150, 100, 0, 1000));
    }

    @Test
    void shiftNegativeSubtractsFromSourceOffset() {
        assertEquals(80, OffsetCalculator.calculate(SHIFT, 100, -20, 0, 1000));
    }

    @Test
    void shiftClampsToEarliestWhenResultWouldGoNegativeOfRange() {
        assertEquals(10, OffsetCalculator.calculate(SHIFT, 20, -100, 10, 1000));
    }

    @Test
    void shiftClampsToLatestWhenResultExceedsRange() {
        assertEquals(1000, OffsetCalculator.calculate(SHIFT, 950, 500, 0, 1000));
    }

    @Test
    void resetToEarliestIgnoresSourceOffset() {
        assertEquals(5, OffsetCalculator.calculate(RESET_EARLIEST, 999, 0, 5, 1000));
    }

    @Test
    void resetToLatestIgnoresSourceOffset() {
        assertEquals(1000, OffsetCalculator.calculate(RESET_LATEST, 5, 0, 5, 1000));
    }
}
