package io.github.borkorajkovic.kafkactl.kafka;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TopicFilterTest {

    private static final Set<String> AVAILABLE = Set.of("orders", "orders-dlq", "payments", "shipping");

    @Test
    void allTopicsSelectsEverything() {
        Set<String> result = TopicFilter.resolve(AVAILABLE, true, null, null, null);
        assertEquals(AVAILABLE, result);
    }

    @Test
    void explicitTopicsSelectsOnlyThose() {
        Set<String> result = TopicFilter.resolve(AVAILABLE, false, List.of("orders", "payments"), null, null);
        assertEquals(Set.of("orders", "payments"), result);
    }

    @Test
    void explicitTopicNotInSourceThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> TopicFilter.resolve(AVAILABLE, false, List.of("unknown-topic"), null, null));
    }

    @Test
    void regexSelectsMatchingTopics() {
        Set<String> result = TopicFilter.resolve(AVAILABLE, false, null, "orders.*", null);
        assertEquals(Set.of("orders", "orders-dlq"), result);
    }

    @Test
    void excludeTopicsSubtractsFromSelection() {
        Set<String> result = TopicFilter.resolve(AVAILABLE, true, null, null, List.of("orders-dlq", "shipping"));
        assertEquals(Set.of("orders", "payments"), result);
    }

    @Test
    void noSelectionModeThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> TopicFilter.resolve(AVAILABLE, false, null, null, null));
    }

    @Test
    void emptyTopicsListIsTreatedAsNoSelection() {
        assertThrows(IllegalArgumentException.class,
                () -> TopicFilter.resolve(AVAILABLE, false, List.of(), null, null));
    }
}
