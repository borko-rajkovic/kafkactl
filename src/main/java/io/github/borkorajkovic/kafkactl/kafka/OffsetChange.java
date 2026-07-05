package io.github.borkorajkovic.kafkactl.kafka;

/**
 * A single partition's planned (or applied) offset change from the source
 * group to the destination group.
 *
 * @param currentTargetOffset the destination group's existing committed offset for
 *                            this partition, or {@code null} if it has none yet
 */
public record OffsetChange(
        String topic,
        int partition,
        long sourceOffset,
        Long currentTargetOffset,
        long newOffset
) {
}
