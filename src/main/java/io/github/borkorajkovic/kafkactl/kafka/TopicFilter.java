package io.github.borkorajkovic.kafkactl.kafka;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Resolves the final set of topics to operate on from the available topics
 * (i.e. those with committed offsets in the source group) and the user's
 * selection options.
 */
public final class TopicFilter {

    private TopicFilter() {
    }

    public static Set<String> resolve(
            Set<String> availableTopics,
            boolean allTopics,
            List<String> topics,
            String regex,
            List<String> excludeTopics
    ) {
        Set<String> selected;

        if (allTopics) {
            selected = new TreeSet<>(availableTopics);
        } else if (regex != null && !regex.isBlank()) {
            Pattern pattern = Pattern.compile(regex);
            selected = availableTopics.stream()
                    .filter(topic -> pattern.matcher(topic).matches())
                    .collect(Collectors.toCollection(TreeSet::new));
        } else if (topics != null && !topics.isEmpty()) {
            selected = new TreeSet<>();
            for (String topic : topics) {
                if (!availableTopics.contains(topic)) {
                    throw new IllegalArgumentException(
                            "Topic '" + topic + "' has no committed offsets in the source group");
                }
                selected.add(topic);
            }
        } else {
            throw new IllegalArgumentException(
                    "One of --all-topics, --topics, or --regex must be specified");
        }

        if (excludeTopics != null && !excludeTopics.isEmpty()) {
            selected.removeAll(excludeTopics);
        }

        return selected;
    }
}
