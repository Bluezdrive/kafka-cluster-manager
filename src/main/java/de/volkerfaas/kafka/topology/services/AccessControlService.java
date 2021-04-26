package de.volkerfaas.kafka.topology.services;

import de.volkerfaas.kafka.topology.model.Domain;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

public interface AccessControlService {

    void createAccessControlLists(Collection<AclBinding> aclBindings) throws ExecutionException, InterruptedException;
    String findPrincipalByResourceName(Collection<AclBinding> aclBindings, String resourceName);
    Collection<String> findPrincipalsByResourceName(Collection<AclBinding> aclBindings, String resourceName);
    Collection<AclBinding> listAclBindingsInCluster() throws ExecutionException, InterruptedException;
    Collection<AclBinding> listNewAclBindings(Collection<Domain> domains);
    Collection<AclBinding> deleteAccessControlLists(Collection<AclBindingFilter> aclBindingFilters) throws ExecutionException, InterruptedException;
    Collection<AclBindingFilter> listOrphanedAclBindings(Collection<Domain> domains) throws ExecutionException, InterruptedException;

}
