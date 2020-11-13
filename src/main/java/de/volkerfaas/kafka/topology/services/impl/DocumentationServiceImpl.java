package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.TopologyFile;
import de.volkerfaas.kafka.topology.services.DocumentationService;
import net.steppschuh.markdowngenerator.table.Table;
import net.steppschuh.markdowngenerator.text.heading.Heading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;

@Repository
public class DocumentationServiceImpl implements DocumentationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentationServiceImpl.class);

    @Override
    public void writeReadmeFile(Set<TopologyFile> topologies) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(buildDomainTable(topologies));
        stringBuilder.append(buildTopicsTable(topologies));

        final String directory = System.getProperty(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DIRECTORY);
        String fileName = directory + "/README.md";
        Files.writeString(Path.of(fileName), stringBuilder.toString(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        LOGGER.info("Documentation in README.md has been updated");
    }

    public String buildDomainTable(Set<TopologyFile> topologies) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(new Heading("Domains", 1)).append("\n");
        Table.Builder tableBuilderDomains = new Table.Builder()
                .withAlignments(Table.ALIGN_LEFT, Table.ALIGN_LEFT, Table.ALIGN_LEFT, Table.ALIGN_LEFT, Table.ALIGN_LEFT)
                .addRow("Domain", "Team", "E-Mail-Address", "Description");
        topologies.forEach(topology -> {
            final Domain domain = topology.getDomain();
            final String team = domain.getMaintainer().getTeam();
            final String email = domain.getMaintainer().getEmail();
            tableBuilderDomains.addRow(domain.getName(), team, email, domain.getDescription());
        });
        stringBuilder.append(tableBuilderDomains.build().toString()).append("\n");

        return stringBuilder.toString();
    }

    public String buildTopicsTable(Set<TopologyFile> topologies) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(new Heading("Topics", 1)).append("\n");
        Table.Builder tableBuilderTopics = new Table.Builder()
                .withAlignments(Table.ALIGN_LEFT, Table.ALIGN_LEFT, Table.ALIGN_LEFT, Table.ALIGN_LEFT, Table.ALIGN_LEFT)
                .addRow("Topic", "Maintainer", "Key Schema File", "Value Schema File", "Description");
        topologies.forEach(topology -> {
            final Domain domain = topology.getDomain();
            final String maintainer = domain.getMaintainer().getTeam();
            domain.getVisibilities()
                    .forEach(visibility -> visibility.getTopics()
                            .forEach(topic -> tableBuilderTopics.addRow(topic.getFullName(), maintainer, topic.getKeySchemaFile(), topic.getValueSchemaFile(), topic.getDescription())));
        });
        stringBuilder.append(tableBuilderTopics.build().toString()).append("\n");

        return stringBuilder.toString();
    }

}
