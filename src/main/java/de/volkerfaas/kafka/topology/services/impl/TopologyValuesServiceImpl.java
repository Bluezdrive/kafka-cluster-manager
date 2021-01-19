package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.topology.model.*;
import de.volkerfaas.kafka.topology.services.TopologyValuesService;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
public class TopologyValuesServiceImpl implements TopologyValuesService {

    @Override
    public TopologyFile addAdditionalValues(final TopologyFile topology) {
        listVisibilitiesWithDomain(topology.getDomain()).stream()
                .peek(this::addAdditionalValuesToVisibility)
                .map(this::listTopicsWithVisibilityAndDomain)
                .flatMap(List::stream)
                .forEach(this::addAdditionalValuesToTopic);

        return topology;
    }

    public void addAdditionalValuesToVisibility(final Pair<Domain, Visibility> pair) {
        final String domainName = pair.getValue0().getName();
        final Visibility visibility = pair.getValue1();
        visibility.setPrefix(domainName + ".");
    }

    public void addAdditionalValuesToTopic(final Triplet<Domain, Visibility, Topic> triplet) {
        final String visibilityFullName = triplet.getValue1().getFullName();
        final Topic topic = triplet.getValue2();
        topic.setPrefix(visibilityFullName + ".");
        final Schema keySchema = topic.getKeySchema();
        addSchemaTopic(keySchema, topic);
        final Schema valueSchema = topic.getValueSchema();
        addSchemaTopic(valueSchema, topic);
    }

    public void addSchemaTopic(final Schema schema, final Topic topic) {
        if (Objects.nonNull(schema)) {
            schema.setTopic(topic);
        }
    }

    public List<Triplet<Domain, Visibility, Topic>> listTopicsWithVisibilityAndDomain(final Pair<Domain, Visibility> pair) {
        return pair.getValue1().getTopics().stream()
                .map(topic -> Triplet.with(pair.getValue0(), pair.getValue1(), topic))
                .collect(Collectors.toUnmodifiableList());
    }

    public List<Pair<Domain, Visibility>> listVisibilitiesWithDomain(final Domain domain) {
        return domain.getVisibilities().stream()
                .map(visibility -> Pair.with(domain, visibility))
                .collect(Collectors.toUnmodifiableList());
    }

}
