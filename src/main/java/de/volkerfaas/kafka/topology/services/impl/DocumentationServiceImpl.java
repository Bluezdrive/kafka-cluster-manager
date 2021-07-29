package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.Schema;
import de.volkerfaas.kafka.topology.model.TopologyFile;
import de.volkerfaas.kafka.topology.services.DocumentationService;
import net.steppschuh.markdowngenerator.table.Table;
import net.steppschuh.markdowngenerator.text.heading.Heading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Repository
public class DocumentationServiceImpl implements DocumentationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentationServiceImpl.class);

    private final boolean dryRun;
    private final String topologyFilename;
    private final String eventsFilename;

    public DocumentationServiceImpl(@Value("${documentation.topology-filename}") final String topologyFilename, @Value("${documentation.events-filename}") final String eventsFilename, @Value("${dry-run:@null}") final String dryRun) {
        this.dryRun = Objects.nonNull(dryRun);
        this.topologyFilename = topologyFilename;
        this.eventsFilename = eventsFilename;
    }

    @Override
    public void writeTopologyDocumentationFile(final Collection<TopologyFile> topologies, final String directory) throws IOException {
        if (dryRun) {
            LOGGER.info("Topology documentation '{}' to be updated", topologyFilename);
        } else {
            final Path path = Path.of(directory, topologyFilename);
            final String content = buildDomainTable(topologies) + "\n" + buildTopicsTable(topologies);
            Files.writeString(path, content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            LOGGER.info("Topology documentation '{}' has been updated", topologyFilename);
        }
    }

    @Override
    public void writeEventsDocumentationFile(final Collection<TopologyFile> topologies, final String directory) throws IOException {
        if (dryRun) {
            LOGGER.info("Events Documentation '{}' to be updated", eventsFilename);
        } else {
            final Path path = Path.of(directory, eventsFilename);
            final StringBuilder stringBuilder = new StringBuilder();
            final String eventsDirectory = directory + "/events";
            topologies.forEach(topology -> buildDomainDocumentation(eventsDirectory, stringBuilder, topology));
            final String content = stringBuilder.toString();
            Files.writeString(path, content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            LOGGER.info("Events documentation '{}' has been updated", topologyFilename);
        }
    }

    private void buildDomainDocumentation(String eventsDirectory, StringBuilder stringBuilder, TopologyFile topology) {
        final Domain domain = topology.getDomain();
        final String domainName = domain.getName();
        stringBuilder.append(new Heading("Domain: " + domainName, 1)).append("\n");
        final Table.Builder tableBuilderEvents = new Table.Builder()
                .withAlignments(Table.ALIGN_LEFT, Table.ALIGN_LEFT, Table.ALIGN_LEFT, Table.ALIGN_LEFT, Table.ALIGN_LEFT, Table.ALIGN_LEFT)
                .addRow("Name", "Type", "File", "Team", "E-Mail-Address", "Description");
        final String domainEventsDirectory = eventsDirectory + "/" + domainName;
        final Path domainEventsPath = Paths.get(domainEventsDirectory);
        if (!Files.exists(domainEventsPath)) {
            LOGGER.info("No events available for domain '{}'", domainName);
            return;
        }
        try(final Stream<Path> pathStream = Files.walk(domainEventsPath, 1)) {
            final List<String> eventFiles = pathStream
                    .filter(p -> !Files.isDirectory(p) && p.toString().endsWith("-value.avsc"))
                    .map(Path::toAbsolutePath)
                    .map(Path::toString)
                    .sorted(String::compareTo)
                    .collect(Collectors.toUnmodifiableList());
            eventFiles.forEach(eventFile -> buildEventDocumentation(domain, tableBuilderEvents, eventFile));
            stringBuilder.append(tableBuilderEvents.build().toString()).append("\n");
            stringBuilder.append("\n");
        } catch (IOException e) {
            LOGGER.error("Error listing directory '{}': ", eventsDirectory, e);
        }
    }

    private void buildEventDocumentation(Domain domain, Table.Builder tableBuilderEvents, String eventFile) {
        final Path eventPath = Path.of(eventFile);
        try {
            final String content = Files.readString(eventPath);
            final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
            final org.apache.avro.Schema schema = parser.parse(content);
            final String name = Objects.equals(org.apache.avro.Schema.Type.RECORD, schema.getType()) ? schema.getName() : "";
            final String doc = schema.getDoc();
            final Path file = eventPath.getFileName();
            tableBuilderEvents.addRow(
                    name,
                    schema.getType().getName(),
                    file,
                    domain.getMaintainer().getName(),
                    domain.getMaintainer().getEmail(),
                    doc
            );
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public String buildDomainTable(final Collection<TopologyFile> topologies) {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(new Heading("Domains", 1)).append("\n");
        final Table.Builder tableBuilderDomains = new Table.Builder()
                .withAlignments(Table.ALIGN_LEFT, Table.ALIGN_LEFT, Table.ALIGN_LEFT, Table.ALIGN_LEFT)
                .addRow("Domain", "Team", "E-Mail-Address", "Description");
        topologies.forEach(topology -> {
            final Domain domain = topology.getDomain();
            final String name = domain.getMaintainer().getName();
            final String email = domain.getMaintainer().getEmail();
            tableBuilderDomains.addRow(
                    domain.getName(),
                    name,
                    email,
                    domain.getDescription());
        });
        stringBuilder.append(tableBuilderDomains.build().toString()).append("\n");

        return stringBuilder.toString();
    }

    public String buildTopicsTable(final Collection<TopologyFile> topologies) {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(new Heading("Topics", 1)).append("\n");
        final Table.Builder tableBuilderTopics = new Table.Builder()
                .withAlignments(Table.ALIGN_LEFT, Table.ALIGN_LEFT, Table.ALIGN_LEFT, Table.ALIGN_LEFT, Table.ALIGN_LEFT)
                .addRow("Topic", "Maintainer", "Key Schema", "Value Schema", "Description");
        topologies.forEach(topology -> {
            final Domain domain = topology.getDomain();
            final String maintainer = domain.getMaintainer().getTeam();
            domain.getVisibilities()
                    .forEach(visibility -> visibility.getTopics()
                            .forEach(topic -> {
                                final Schema keySchema = topic.getKeySchema();
                                final Schema valueSchema = topic.getValueSchema();
                                tableBuilderTopics.addRow(
                                        topic.getFullName(),
                                        maintainer,
                                        Objects.nonNull(keySchema) ? keySchema.getSubject() : "",
                                        Objects.nonNull(valueSchema) ? valueSchema.getSubject() : "",
                                        topic.getDescription());
                            }));
        });
        stringBuilder.append(tableBuilderTopics.build().toString()).append("\n");

        return stringBuilder.toString();
    }

}
