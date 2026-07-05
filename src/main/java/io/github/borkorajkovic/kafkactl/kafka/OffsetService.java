package io.github.borkorajkovic.kafkactl.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Thin wrapper around Kafka's {@link AdminClient} for the offset operations
 * {@code copy-offsets} needs. Kept free of CLI/output concerns so it can be
 * exercised independently of {@code CopyOffsetsCommand}.
 */
public final class OffsetService implements AutoCloseable {

    private final AdminClient adminClient;

    public OffsetService(Properties clientConfig) {
        this.adminClient = AdminClient.create(clientConfig);
    }

    public Map<TopicPartition, Long> fetchGroupOffsets(String groupId) throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetAndMetadata> raw = adminClient.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get();

        Map<TopicPartition, Long> offsets = new HashMap<>();
        raw.forEach((partition, offsetAndMetadata) -> offsets.put(partition, offsetAndMetadata.offset()));
        return offsets;
    }

    public Map<TopicPartition, Long> fetchEarliestOffsets(Set<TopicPartition> partitions) throws ExecutionException, InterruptedException {
        return fetchOffsets(partitions, OffsetSpec.earliest());
    }

    public Map<TopicPartition, Long> fetchLatestOffsets(Set<TopicPartition> partitions) throws ExecutionException, InterruptedException {
        return fetchOffsets(partitions, OffsetSpec.latest());
    }

    public void alterGroupOffsets(String groupId, Map<TopicPartition, Long> newOffsets) throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetAndMetadata> request = newOffsets.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> new OffsetAndMetadata(entry.getValue())));
        adminClient.alterConsumerGroupOffsets(groupId, request).all().get();
    }

    private Map<TopicPartition, Long> fetchOffsets(
            Set<TopicPartition> partitions, OffsetSpec spec
    ) throws ExecutionException, InterruptedException {
        if (partitions.isEmpty()) {
            return Map.of();
        }

        Map<TopicPartition, OffsetSpec> request = partitions.stream()
                .collect(Collectors.toMap(partition -> partition, partition -> spec));
        ListOffsetsResult result = adminClient.listOffsets(request);

        Map<TopicPartition, Long> offsets = new HashMap<>();
        for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : result.all().get().entrySet()) {
            offsets.put(entry.getKey(), entry.getValue().offset());
        }
        return offsets;
    }

    @Override
    public void close() {
        adminClient.close();
    }
}
