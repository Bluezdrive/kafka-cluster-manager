package de.volkerfaas.kafka.topology.repositories.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.model.TopologyFile;
import de.volkerfaas.kafka.topology.repositories.TopologyFileRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Repository
public class TopologyFileRepositoryImpl implements TopologyFileRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyFileRepositoryImpl.class);

    @Override
    public TopologyFile readTopology(String pathname) {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            final String content = Files.readString(Path.of(pathname));
            if (Objects.nonNull(content) && !content.isBlank()) {
                TopologyFile topology = mapper.readValue(content, TopologyFile.class);
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
    public TopologyFile writeTopology(TopologyFile topology) {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final File file = topology.getFile();
        try {
            mapper.writeValue(file, topology);
            LOGGER.info("Wrote topology to {}", file.getAbsolutePath());
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Error writing file '%s': %s", file.getAbsolutePath(), e.getMessage()), e);
        }

        return topology;
    }

    @Override
    public Set<String> listTopologyFiles(String directory, List<String> domainNames) {
        try {
            try(final Stream<Path> pathStream = Files.walk(Paths.get(directory), 1)) {
                final Set<String> topologyFiles = pathStream
                        .filter(file -> isTopologyFile(file, domainNames))
                        .map(Path::toAbsolutePath)
                        .map(Path::toString)
                        .collect(Collectors.toSet());
                LOGGER.debug("Topology files: {}", topologyFiles);
                return topologyFiles;
            }
        } catch (IOException e) {
            LOGGER.error("Error listing directory '{}': ", directory, e);
            return Collections.emptySet();
        }
    }

    boolean isTopologyFile(Path file, List<String> domainNames) {
        if (Files.isDirectory(file)) {
            return false;
        }
        final String filePath = file.getFileName().toString();
        final Pattern pattern = Pattern.compile(ApplicationConfiguration.REGEX_TOPOLOGY_FILENAME);
        final Matcher matcher = pattern.matcher(filePath);
        if (!matcher.matches() || matcher.groupCount() != 1) {
            return false;
        }

        return domainNames.isEmpty() || domainNames.contains(matcher.group(1));
    }

}
