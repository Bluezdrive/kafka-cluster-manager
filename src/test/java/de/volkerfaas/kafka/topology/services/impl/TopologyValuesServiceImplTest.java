package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.topology.model.*;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisplayName("In the class TopologyValuesServiceImpl")
public class TopologyValuesServiceImplTest {

    private TopologyValuesServiceImpl topologyValuesService;

    @BeforeEach
    void init() {
        this.topologyValuesService = new TopologyValuesServiceImpl();
    }

    @Nested
    @DisplayName("the method addAdditionalValues")
    class AddAdditionalValues {

        @Test
        @DisplayName("should return a topology with prefix added to visibilities and topics")
        void testAddAdditionalValues() {
            final TopologyFile topology = new TopologyFile();

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            domain.getVisibilities().add(visibility);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            visibility.getTopics().add(topic);

            final TopologyFile topologyWithAdditionalValues = topologyValuesService.addAdditionalValues(topology);
            assertNotNull(topologyWithAdditionalValues);
            final Domain domainWithAdditionalValues = topologyWithAdditionalValues.getDomain();
            assertNotNull(domainWithAdditionalValues);
            final Visibility visibilityWithAdditionalValues = domainWithAdditionalValues.getVisibilities().stream().findFirst().orElse(null);
            assertNotNull(visibilityWithAdditionalValues);
            assertEquals(domain.getName() + ".", visibilityWithAdditionalValues.getPrefix());
            final Topic topicWithAdditionalValues = visibilityWithAdditionalValues.getTopics().stream().findFirst().orElse(null);
            assertNotNull(topicWithAdditionalValues);
            assertEquals(visibility.getFullName() + ".", topicWithAdditionalValues.getPrefix());
        }

    }

    @Nested
    @DisplayName("the method addAdditionalValuesToTopic")
    class AddAdditionalValuesToTopic {

        @Test
        @DisplayName("should add the prefix to a topic")
        void testAddAdditionalValuesToTopic() {
            final String domainName = "de.volkerfaas.arc";
            final String visibilityFullName = "de.volkerfaas.arc.public";

            final Domain domain = new Domain(domainName, "User:973456");
            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix(domainName + ".");
            domain.getVisibilities().add(visibility);
            final Topic topic = new Topic();
            topic.setName("user_updated");
            visibility.getTopics().add(topic);

            final Triplet<Domain, Visibility, Topic> triplet = Triplet.with(domain, visibility, topic);
            topologyValuesService.addAdditionalValuesToTopic(triplet);
            assertEquals(visibilityFullName + ".", topic.getPrefix());
        }

        @Test
        @DisplayName("should add the value schema type AVRO to a topic when a schema without type is given")
        void testAddAdditionalValuesToTopicWithValueSchemaAvro() {
            final String domainName = "de.volkerfaas.arc";
            final String visibilityFullName = "de.volkerfaas.arc.public";

            final Domain domain = new Domain(domainName, "User:973456");
            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix(domainName + ".");
            domain.getVisibilities().add(visibility);
            final Topic topic = new Topic();
            topic.setName("user_updated");
            visibility.getTopics().add(topic);
            final Schema schema = new Schema();
            schema.setFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc");
            topic.setValueSchema(schema);

            final Triplet<Domain, Visibility, Topic> triplet = Triplet.with(domain, visibility, topic);
            topologyValuesService.addAdditionalValuesToTopic(triplet);
            assertEquals(Schema.Type.AVRO, schema.getType());
        }

        @Test
        @DisplayName("should add the value schema type PROTOBUF to a topic when a schema without type is given")
        void testAddAdditionalValuesToTopicWithValueSchemaProtobuf() {
            final String domainName = "de.volkerfaas.arc";
            final String visibilityFullName = "de.volkerfaas.arc.public";

            final Domain domain = new Domain(domainName, "User:973456");
            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix(domainName + ".");
            domain.getVisibilities().add(visibility);
            final Topic topic = new Topic();
            topic.setName("user_updated");
            visibility.getTopics().add(topic);
            final Schema schema = new Schema();
            schema.setFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.proto");
            topic.setValueSchema(schema);

            final Triplet<Domain, Visibility, Topic> triplet = Triplet.with(domain, visibility, topic);
            topologyValuesService.addAdditionalValuesToTopic(triplet);
            assertEquals(Schema.Type.PROTOBUF, schema.getType());
        }

        @Test
        @DisplayName("should add the value schema type JSON to a topic when a schema without type is given")
        void testAddAdditionalValuesToTopicWithValueSchemaJson() {
            final String domainName = "de.volkerfaas.arc";
            final String visibilityFullName = "de.volkerfaas.arc.public";

            final Domain domain = new Domain(domainName, "User:973456");
            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix(domainName + ".");
            domain.getVisibilities().add(visibility);
            final Topic topic = new Topic();
            topic.setName("user_updated");
            visibility.getTopics().add(topic);
            final Schema schema = new Schema();
            schema.setFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.json");
            topic.setValueSchema(schema);

            final Triplet<Domain, Visibility, Topic> triplet = Triplet.with(domain, visibility, topic);
            topologyValuesService.addAdditionalValuesToTopic(triplet);
            assertEquals(Schema.Type.JSON, schema.getType());
        }

        @Test
        @DisplayName("should add the key schema type AVRO to a topic when a schema without type is given")
        void testAddAdditionalValuesToTopicWithKeySchemaAvro() {
            final String domainName = "de.volkerfaas.arc";
            final String visibilityFullName = "de.volkerfaas.arc.public";

            final Domain domain = new Domain(domainName, "User:973456");
            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix(domainName + ".");
            domain.getVisibilities().add(visibility);
            final Topic topic = new Topic();
            topic.setName("user_updated");
            visibility.getTopics().add(topic);
            final Schema schema = new Schema();
            schema.setFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc");
            topic.setKeySchema(schema);

            final Triplet<Domain, Visibility, Topic> triplet = Triplet.with(domain, visibility, topic);
            topologyValuesService.addAdditionalValuesToTopic(triplet);
            assertEquals(Schema.Type.AVRO, schema.getType());
        }

        @Test
        @DisplayName("should add the key schema type PROTOBUF to a topic when a schema without type is given")
        void testAddAdditionalValuesToTopicWithKeySchemaProtobuf() {
            final String domainName = "de.volkerfaas.arc";
            final String visibilityFullName = "de.volkerfaas.arc.public";

            final Domain domain = new Domain(domainName, "User:973456");
            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix(domainName + ".");
            domain.getVisibilities().add(visibility);
            final Topic topic = new Topic();
            topic.setName("user_updated");
            visibility.getTopics().add(topic);
            final Schema schema = new Schema();
            schema.setFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.proto");
            topic.setKeySchema(schema);

            final Triplet<Domain, Visibility, Topic> triplet = Triplet.with(domain, visibility, topic);
            topologyValuesService.addAdditionalValuesToTopic(triplet);
            assertEquals(Schema.Type.PROTOBUF, schema.getType());
        }

        @Test
        @DisplayName("should add the key schema type JSON to a topic when a schema without type is given")
        void testAddAdditionalValuesToTopicWithKeySchemaJson() {
            final String domainName = "de.volkerfaas.arc";
            final String visibilityFullName = "de.volkerfaas.arc.public";

            final Domain domain = new Domain(domainName, "User:973456");
            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix(domainName + ".");
            domain.getVisibilities().add(visibility);
            final Topic topic = new Topic();
            topic.setName("user_updated");
            visibility.getTopics().add(topic);
            final Schema schema = new Schema();
            schema.setFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.json");
            topic.setKeySchema(schema);

            final Triplet<Domain, Visibility, Topic> triplet = Triplet.with(domain, visibility, topic);
            topologyValuesService.addAdditionalValuesToTopic(triplet);
            assertEquals(Schema.Type.JSON, schema.getType());
        }

    }

    @Nested
    @DisplayName("the method addAdditionalValuesToVisibility")
    class AddAdditionalValuesToVisibility {

        @Test
        @DisplayName("should add the prefix to a visibility")
        void testAddAdditionalValuesToVisibility() {
            final String domainName = "de.volkerfaas.arc";

            final Domain domain = new Domain(domainName, "User:973456");
            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            domain.getVisibilities().add(visibility);

            final Pair<Domain, Visibility> pair = Pair.with(domain, visibility);
            topologyValuesService.addAdditionalValuesToVisibility(pair);
            assertEquals(domainName + ".", visibility.getPrefix());
        }

    }

    @Nested
    @DisplayName("the method listVisibilitiesWithDomain")
    class ListVisibilitiesWithDomain {

        @Test
        @DisplayName("should return a list of pairs with domain name and visibility")
        void testListVisibilitiesWithDomain() {
            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            domain.setPrincipal("User:129849");

            final Visibility publicVisibility = new Visibility();
            publicVisibility.setType(Visibility.Type.PUBLIC);
            domain.getVisibilities().add(publicVisibility);

            final Visibility privateVisibility = new Visibility();
            privateVisibility.setType(Visibility.Type.PRIVATE);
            domain.getVisibilities().add(privateVisibility);

            final List<Pair<Domain, Visibility>> pairs = topologyValuesService.listVisibilitiesWithDomain(domain);
            assertNotNull(pairs);
            assertEquals(2, pairs.size());
            assertThat(pairs, hasItems(
                    Pair.with(domain, publicVisibility),
                    Pair.with(domain, privateVisibility)
            ));
        }

    }

    @Nested
    @DisplayName("the method listTopicsWithVisibilityAndDomain")
    class ListTopicsWithVisibilityAndDomain {

        @Test
        @DisplayName("should return a list of triplets containing domain and visibility and topic")
        void listListTopicsWithVisibilityAndDomain() {
            final String domainName = "de.volkerfaas.arc";

            final Domain domain = new Domain(domainName, "User:973456");

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            final Topic topicUserUpdated = new Topic();
            topicUserUpdated.setName("user_updated");
            visibility.getTopics().add(topicUserUpdated);

            final Topic topicCardUpdated = new Topic();
            topicCardUpdated.setName("card_updated");
            visibility.getTopics().add(topicCardUpdated);

            final Pair<Domain, Visibility> pair = Pair.with(domain, visibility);
            final List<Triplet<Domain, Visibility, Topic>> triplets = topologyValuesService.listTopicsWithVisibilityAndDomain(pair);
            assertNotNull(triplets);
            assertEquals(2, triplets.size());
            assertThat(triplets, hasItems(
                    Triplet.with(domain, visibility, topicUserUpdated),
                    Triplet.with(domain, visibility, topicCardUpdated)
            ));
        }

    }

}
