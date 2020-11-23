package de.volkerfaas.kafka.topology.repositories.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.model.TopologyFile;
import de.volkerfaas.kafka.topology.repositories.TopologyFileRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Repository
public class TopologyFileRepositoryImpl implements TopologyFileRepository {

    private static final Pattern PATTERN_TOPOLOGY_FILENAME = Pattern.compile(ApplicationConfiguration.REGEX_TOPOLOGY_FILENAME);
    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyFileRepositoryImpl.class);

    private final ObjectMapper objectMapper;
    private final boolean dryRun;

    @Autowired
    public TopologyFileRepositoryImpl(final ObjectMapper objectMapper, @Value("${dry-run:@null}") final String dryRun) {
        this.dryRun = Objects.nonNull(dryRun);
        this.objectMapper = objectMapper;
    }

    @Override
    public TopologyFile readTopology(final String pathname) {
        try {
            final String content = Files.readString(Path.of(pathname));
            if (Objects.nonNull(content) && !content.isBlank()) {
                final TopologyFile topology = objectMapper.readValue(content, TopologyFile.class);
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

    public boolean isTopologyFile(final Path file) {
        if (Files.isDirectory(file)) {
            return false;
        }
        final String filePath = file.getFileName().toString();
        final Matcher matcher = PATTERN_TOPOLOGY_FILENAME.matcher(filePath);

        return matcher.matches() && matcher.groupCount() == 1;
    }

}
