package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.Team;
import de.volkerfaas.kafka.topology.model.TopologyFile;
import de.volkerfaas.kafka.topology.repositories.TopologyFileRepository;
import de.volkerfaas.kafka.topology.services.TopologyCreateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import java.io.File;
import java.util.Set;

@Service
public class TopologyCreateServiceImpl implements TopologyCreateService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyCreateServiceImpl.class);

    private final TopologyFileRepository topologyFileRepository;
    private final Validator validator;

    @Autowired
    public TopologyCreateServiceImpl(TopologyFileRepository topologyFileRepository, Validator validator) {
        this.topologyFileRepository = topologyFileRepository;
        this.validator = validator;
    }

    @Override
    public void createTopology(final String directory, final String domainName, final String description, final String maintainerName, final String maintainerEmail, final String serviceAccountId) {
        final Domain domain = createDomain(domainName, description, maintainerName, maintainerEmail, serviceAccountId);
        if (!isDomainValid(domain)) {
            return;
        }

        createTopologyFile(directory, domain);
    }

    private void createTopologyFile(String directory, Domain domain) {
        final File file = new File(directory, "topology-" + domain.getName() + ".yaml");
        final TopologyFile topology = new TopologyFile();
        topology.setFile(file);
        topology.setDomain(domain);

        topologyFileRepository.writeTopology(topology, false);
    }

    private boolean isDomainValid(Domain domain) {
        final Set<ConstraintViolation<Domain>> result = validator.validate(domain);
        if (!result.isEmpty()) {
            result.forEach(violation -> LOGGER.error("{} {}", violation.getPropertyPath(), violation.getMessage()));
            return false;
        }

        return true;
    }

    private Domain createDomain(String domainName, String description, String maintainerName, String maintainerEmail, String serviceAccountId) {
        final String principal = "User:" + serviceAccountId;
        final Domain domain = new Domain(domainName, principal);
        domain.setDescription(description);

        final Team maintainer = new Team();
        maintainer.setName(maintainerName);
        maintainer.setEmail(maintainerEmail);
        domain.setMaintainer(maintainer);

        return domain;
    }
}
