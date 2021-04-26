package de.volkerfaas.kafka.topology.model;

import de.volkerfaas.kafka.topology.validation.PrincipalExists;

import javax.validation.constraints.Pattern;

import static de.volkerfaas.kafka.topology.ApplicationConfiguration.REGEX_DOMAIN;
import static de.volkerfaas.kafka.topology.ApplicationConfiguration.REGEX_PRINCIPAL;

public class AccessControl {

    private String domain;
    private String principal;

    public AccessControl() {

    }

    public AccessControl(String principal) {
        this.principal = principal;
    }

    @Pattern(
            message = "must be '(country).(company).(domain)'",
            regexp = "^" + REGEX_DOMAIN + "$"
    )
    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
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
