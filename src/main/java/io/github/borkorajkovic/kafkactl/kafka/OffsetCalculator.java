package io.github.borkorajkovic.kafkactl.kafka;

/**
 * Computes the target offset for a single partition given the requested mode.
 * Copied and shifted offsets are clamped to {@code [earliestOffset, latestOffset]}
 * so a stale source offset or an aggressive shift never produces an offset that
 * Kafka would reject.
 */
public final class OffsetCalculator {

    private OffsetCalculator() {
    }

    public enum Mode {
        COPY,
        SHIFT,
        RESET_EARLIEST,
        RESET_LATEST
    }

    public static long calculate(
            Mode mode,
            long sourceOffset,
            long shiftAmount,
            long earliestOffset,
            long latestOffset
    ) {
        long result = switch (mode) {
            case COPY -> sourceOffset;
            case SHIFT -> sourceOffset + shiftAmount;
            case RESET_EARLIEST -> earliestOffset;
            case RESET_LATEST -> latestOffset;
        };

        if (mode == Mode.COPY || mode == Mode.SHIFT) {
            result = Math.max(earliestOffset, Math.min(latestOffset, result));
        }

        return result;
    }
}
