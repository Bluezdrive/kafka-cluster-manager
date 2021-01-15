package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.TopologyFile;
import de.volkerfaas.kafka.topology.repositories.TopologyFileRepository;
import de.volkerfaas.kafka.topology.services.AccessControlService;
import de.volkerfaas.kafka.topology.services.TopologyValuesService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import javax.validation.Validator;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

@DisplayName("In the class TopologyBuildServiceImpl")
class TopologyBuildServiceImplTest {

    private TopologyBuildServiceImpl topologyBuildService;

    @BeforeEach
    void init() {
        final AccessControlService accessControlService = mock(AccessControlService.class);
        final SchemaFileServiceImpl schemaFileService = mock(SchemaFileServiceImpl.class);
        final TopicServiceImpl topicService = mock(TopicServiceImpl.class);
        final TopologyFileRepository topologyFileRepository = mock(TopologyFileRepository.class);
        final TopologyValuesService topologyValuesService = new TopologyValuesServiceImpl();
        final Validator validator = mock(Validator.class);
        this.topologyBuildService = new TopologyBuildServiceImpl(accessControlService, schemaFileService, topicService, topologyValuesService, topologyFileRepository, validator);
    }

    @Nested
    @DisplayName("the method listDomainsForUpdate")
    class ListDomainsForUpdate {

        @Test
        @DisplayName("should return the domains of all topology files when no domain names are set")
        void testListDomainsForUpdate() {
            final TopologyFile topologyFileArc = new TopologyFile();
            final Domain domainArc = new Domain();
            domainArc.setName("de.volkerfaas.arc");
            topologyFileArc.setDomain(domainArc);

            final TopologyFile topologyFileFoo = new TopologyFile();
            final Domain domainFoo = new Domain();
            domainFoo.setName("de.volkerfaas.foo");
            topologyFileFoo.setDomain(domainFoo);

            final TopologyFile topologyFileBar = new TopologyFile();
            final Domain domainBar = new Domain();
            domainBar.setName("de.volkerfaas.bar");
            topologyFileBar.setDomain(domainBar);

            final Set<TopologyFile> topologyFiles = Set.of(topologyFileArc, topologyFileFoo, topologyFileBar);
            final Collection<Domain> domains = topologyBuildService.filterDomainsForUpdate(topologyFiles, Collections.emptyList());
            assertEquals(3, domains.size());
            assertThat(domains, hasItems(domainArc, domainFoo, domainBar));
        }

        @Test
        @DisplayName("should only include the domain listed")
        void testListDomainsForUpdateSelectOne() {
            final TopologyFile topologyFileArc = new TopologyFile();
            final Domain domainArc = new Domain();
            domainArc.setName("de.volkerfaas.arc");
            topologyFileArc.setDomain(domainArc);

            final TopologyFile topologyFileFoo = new TopologyFile();
            final Domain domainFoo = new Domain();
            domainFoo.setName("de.volkerfaas.foo");
            topologyFileFoo.setDomain(domainFoo);

            final TopologyFile topologyFileBar = new TopologyFile();
            final Domain domainBar = new Domain();
            domainBar.setName("de.volkerfaas.bar");
            topologyFileBar.setDomain(domainBar);

            final Set<TopologyFile> topologyFiles = Set.of(topologyFileArc, topologyFileFoo, topologyFileBar);
            final List<String> domainNames = List.of("de.volkerfaas.arc");
            final Collection<Domain> domains = topologyBuildService.filterDomainsForUpdate(topologyFiles, domainNames);
            assertEquals(1, domains.size());
            assertThat(domains, hasItems(domainArc));
        }

        @Test
        @DisplayName("should only the domains listed")
        void testListDomainsForUpdateSelectMultiple() {
            final TopologyFile topologyFileArc = new TopologyFile();
            final Domain domainArc = new Domain();
            domainArc.setName("de.volkerfaas.arc");
            topologyFileArc.setDomain(domainArc);

            final TopologyFile topologyFileFoo = new TopologyFile();
            final Domain domainFoo = new Domain();
            domainFoo.setName("de.volkerfaas.foo");
            topologyFileFoo.setDomain(domainFoo);

            final TopologyFile topologyFileBar = new TopologyFile();
            final Domain domainBar = new Domain();
            domainBar.setName("de.volkerfaas.bar");
            topologyFileBar.setDomain(domainBar);

            final Set<TopologyFile> topologyFiles = Set.of(topologyFileArc, topologyFileFoo, topologyFileBar);
            final List<String> domainNames = List.of("de.volkerfaas.arc", "de.volkerfaas.bar");
            final Collection<Domain> domains = topologyBuildService.filterDomainsForUpdate(topologyFiles, domainNames);
            assertEquals(2, domains.size());
            assertThat(domains, hasItems(domainArc, domainBar));
        }

    }

}
