package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.TopologyFile;
import de.volkerfaas.kafka.topology.repositories.TopologyFileRepository;
import de.volkerfaas.kafka.topology.services.TopologyDeleteService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@Service
public class TopologyDeleteServiceImpl implements TopologyDeleteService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyDeleteServiceImpl.class);
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private final TopologyFileRepository topologyFileRepository;

    @Autowired
    public TopologyDeleteServiceImpl(final TopologyFileRepository topologyFileRepository) {
        this.topologyFileRepository = topologyFileRepository;
    }

    @Override
    public void deleteTopology(final String directory, final Collection<String> domainNames) {
        final Set<String> topologyFileNames = topologyFileRepository.listTopologyFiles(directory);
        final Set<TopologyFile> topologyFiles = readTopologyFiles(topologyFileNames);
        final Set<Domain> domains = readDomains(topologyFiles, domainNames);
        if (Objects.isNull(domains) || domains.isEmpty()) {
            LOGGER.info("Domains '{}' not found in topology", domainNames);
        }

        final String backupFileSuffix = "." + LocalDateTime.now().format(DATE_TIME_FORMATTER);
        moveFiles(directory, domains, backupFileSuffix);
        final Set<String> principals = domains.stream()
                .map(Domain::getPrincipal)
                .collect(Collectors.toUnmodifiableSet());
        removePrincipalsFromConfigurationFiles(directory, principals, backupFileSuffix);
    }

    private void removePrincipalsFromConfigurationFiles(final String directory, final Set<String> principals, final String backupFileSuffix) {
        final Set<String> topologyFileNames = topologyFileRepository.listTopologyFiles(directory);
        topologyFileNames.forEach(topologyFileName -> {
            final Path path = Path.of(topologyFileName);
            try {
                if (hasOneOfPrincipals(Files.readString(path), principals)) {
                    Files.copy(path, Path.of(topologyFileName + backupFileSuffix), REPLACE_EXISTING);
                    final String content = Files.lines(path)
                            .filter(line -> !hasOneOfPrincipals(line, principals))
                            .reduce("", (lines, line) -> lines + line + System.lineSeparator());
                    Files.writeString(path, content);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private void moveFiles(final String directory, final Collection<Domain> domains, final String backupFileSuffix) {
        domains.stream()
                .map(domain -> directory + "/topology-" + domain.getName() + ".yaml")
                .forEach(topologyFileName -> {
                    final Path path = Path.of(topologyFileName);
                    if (Files.exists(path)) {
                        try {
                            Files.move(path, Path.of(topologyFileName + backupFileSuffix));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
    }

    private boolean hasOneOfPrincipals(String line, Set<String> principals) {
        return principals.stream()
                .map(line::contains)
                .reduce(true, (previous, current) -> previous && current);
    }

    private Set<TopologyFile> readTopologyFiles(Set<String> topologyFileNames) {
        return topologyFileNames.stream()
                .map(topologyFileRepository::readTopology)
                .filter(Objects::nonNull)
                .collect(Collectors.toUnmodifiableSet());
    }

    private Set<Domain> readDomains(final Set<TopologyFile> topologyFiles, final Collection<String> domainNames) {
        return topologyFiles.stream()
                .map(TopologyFile::getDomain)
                .filter(d -> domainNames.contains(d.getName()))
                .collect(Collectors.toUnmodifiableSet());
    }

}
