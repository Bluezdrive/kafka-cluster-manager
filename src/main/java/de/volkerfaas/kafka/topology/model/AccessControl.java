package de.volkerfaas.kafka.topology.model;

import de.volkerfaas.kafka.topology.validation.PrincipalExists;

import javax.validation.constraints.Pattern;

public class AccessControl {

    public static final String PRINCIPAL_REGEX = "^(User)+\\:([0-9]+)*$";

    private String principal;

    public AccessControl() {

    }

    public AccessControl(String principal) {
        this.principal = principal;
    }

    @PrincipalExists
    @Pattern(regexp = PRINCIPAL_REGEX)
    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    @Override
    public String toString() {
        return "AccessControl{" +
                "principal='" + principal + '\'' +
                '}';
    }
}
