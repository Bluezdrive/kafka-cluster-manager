package de.volkerfaas.kafka.topology.services;

import de.volkerfaas.kafka.topology.model.Domain;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public interface AccessControlService {

    void createAccessControlLists(Collection<AclBinding> aclBindings) throws ExecutionException, InterruptedException;
    Set<AclBinding> listNewAclBindings(List<Domain> domains);
    void deleteAccessControlLists(Collection<AclBindingFilter> aclBindingFilters);
    Set<AclBindingFilter> listOrphanedAclBindings(List<Domain> domains) throws ExecutionException, InterruptedException;

}
