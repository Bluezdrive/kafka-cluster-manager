package de.volkerfaas.kafka.topology.model;

import de.volkerfaas.kafka.topology.validation.PrincipalExists;

import javax.validation.constraints.Pattern;

import static de.volkerfaas.kafka.topology.ApplicationConfiguration.REGEX_PRINCIPAL;

public class AccessControl {

    private String principal;

    public AccessControl() {

    }

    public AccessControl(String principal) {
        this.principal = principal;
    }

    @PrincipalExists
    @Pattern(
            message = "must be 'User:(service-account-id)'",
            regexp = REGEX_PRINCIPAL
    )
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
