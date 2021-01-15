package de.volkerfaas.kafka.topology.repositories.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.model.*;
import de.volkerfaas.kafka.topology.repositories.TopologyFileRepository;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.freva.asciitable.HorizontalAlign.LEFT;

@Repository
public class TopologyFileRepositoryImpl implements TopologyFileRepository {

    private static final Pattern PATTERN_TOPOLOGY_FILENAME = Pattern.compile(ApplicationConfiguration.REGEX_TOPOLOGY_FILENAME);
    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyFileRepositoryImpl.class);

    private final ObjectMapper objectMapper;
    private final String cluster;
    private final boolean dryRun;

    @Autowired
    public TopologyFileRepositoryImpl(final ObjectMapper objectMapper, @Value("${cluster:@null}") final String cluster, @Value("${dry-run:@null}") final String dryRun) {
        this.objectMapper = objectMapper;
        this.cluster = cluster;
        this.dryRun = Objects.nonNull(dryRun);
    }

    @Override
    public TopologyFile readTopology(final String pathname) {
        try {
            final String content = Files.readString(Path.of(pathname));
            if (Objects.nonNull(content) && !content.isBlank()) {
                final Map<String, String> config = readConfig(pathname);
                final String contentWithAppliedConfig = applyConfig(content, config);
                final TopologyFile topology = objectMapper.readValue(contentWithAppliedConfig, TopologyFile.class);
                topology.setFile(new File(pathname));
                LOGGER.debug("Successfully parsed file '{}'", pathname);
                return topology;
            } else {
                throw new IllegalArgumentException("Cannot parse empty file '" + pathname + "'");
            }
        } catch (InvalidFormatException e) {
            throw new IllegalArgumentException(String.format("Error parsing file '%s': %s", pathname, e.getMessage()), e);
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Error reading file '%s': %s", pathname, e.getMessage()), e);
        }
    }

    public String applyConfig(String content, Map<String, String> config) {
        for (Map.Entry<String, String> entry : config.entrySet()) {
            content = content.replaceAll(entry.getKey(), entry.getValue());
        }
        return content;
    }

    @Override
    public TopologyFile writeTopology(final TopologyFile topology) {
        final File file = topology.getFile();
        if (dryRun) {
            LOGGER.info("Topology to be written to {}", file.getAbsolutePath());
        } else {
            try {
                objectMapper.writeValue(file, topology);
                LOGGER.info("Topology written to {}", file.getAbsolutePath());
            } catch (IOException e) {
                throw new IllegalStateException(String.format("Error writing file '%s': %s", file.getAbsolutePath(), e.getMessage()), e);
            }
        }
        printTopology(topology);

        return topology;
    }

    @Override
    public Set<String> listTopologyFiles(final String directory) {
        try(final Stream<Path> pathStream = Files.walk(Paths.get(directory), 1)) {
            final Set<String> topologyFiles = pathStream
                    .filter(this::isTopologyFile)
                    .map(Path::toAbsolutePath)
                    .map(Path::toString)
                    .collect(Collectors.toSet());
            LOGGER.debug("Topology files: {}", topologyFiles);
            return topologyFiles;
        } catch (IOException e) {
            LOGGER.error("Error listing directory '{}': ", directory, e);
            return Collections.emptySet();
        }
    }

    public Map<String, String> filterConfigByCluster(final Map<String, List<ConfigEntry>> config) {
        return config.entrySet().stream()
                .map(this::filterConfigItemByCluster)
                .filter(Objects::nonNull)
                .collect(Collectors.toUnmodifiableMap(Pair::getValue0, Pair::getValue1));
    }

    public Pair<String, String> filterConfigItemByCluster(Map.Entry<String, List<ConfigEntry>> item) {
        return item.getValue().stream()
                .filter(entry -> entry.getClusters().contains(cluster) || entry.getClusters().isEmpty())
                .sorted(Comparator.comparingInt(entry -> -entry.getClusters().size()))
                .map(ConfigEntry::getValue)
                .filter(Objects::nonNull)
                .map(entry -> Pair.with(item.getKey(), entry))
                .findFirst()
                .orElse(null);
    }

    public boolean isTopologyFile(final Path file) {
        if (Files.isDirectory(file)) {
            return false;
        }
        final String filePath = file.getFileName().toString();
        final Matcher matcher = PATTERN_TOPOLOGY_FILENAME.matcher(filePath);

        return matcher.matches() && matcher.groupCount() == 1;
    }

    private void printTopology(final TopologyFile topology) {
        if (Objects.isNull(topology) || Objects.isNull(topology.getDomain())) {
            return;
        }
        System.out.println(AsciiTable.getTable(Collections.singletonList(topology), Arrays.asList(
                new Column().header("Topology File").dataAlign(LEFT).with(t -> t.getFile().getName()),
                new Column().header("Domain").dataAlign(LEFT).with(t -> t.getDomain().getName()),
                new Column().header("Principal").dataAlign(LEFT).with(t -> t.getDomain().getPrincipal())
        )));
        final List<Topic> topics = topology.getDomain().getVisibilities().stream().map(Visibility::getTopics).flatMap(List::stream).collect(Collectors.toUnmodifiableList());
        System.out.println(AsciiTable.getTable(topics, Arrays.asList(
                new Column().header("Topic").dataAlign(LEFT).with(Topic::getFullName),
                new Column().header("Partitions").dataAlign(LEFT).with(topic -> String.valueOf(topic.getNumPartitions())),
                new Column().header("Replication Factor").dataAlign(LEFT).with(topic -> String.valueOf(topic.getReplicationFactor())),
                new Column().header("Key Schema").dataAlign(LEFT).with(topic -> {
                    final Schema keySchema = topic.getKeySchema();
                    if (Objects.isNull(keySchema)) return null;
                    return keySchema.getSubject();
                }),
                new Column().header("Value Schema").dataAlign(LEFT).with(topic -> {
                    final Schema valueSchema = topic.getValueSchema();
                    if (Objects.isNull(valueSchema)) return null;
                    return valueSchema.getSubject();
                })

        )));
    }

    public Map<String, String> readConfig(final String pathname) {
        final TypeReference<Map<String, List<ConfigEntry>>> typeReference = new TypeReference<>() {};
        final String configPathname = pathname.replaceFirst("/topology-", "/config/config-");
        try {
            final String content = Files.readString(Path.of(configPathname));
            final Map<String, List<ConfigEntry>> config = objectMapper.readValue(content, typeReference);
            if (Objects.isNull(config)) {
                return Collections.emptyMap();
            }
            return filterConfigByCluster(config);
        } catch (NoSuchFileException e) {
            LOGGER.debug("No configuration file for topology file {}", pathname);
            return Collections.emptyMap();
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Error reading file '%s': %s", pathname, e.getMessage()), e);
        }
    }

}
