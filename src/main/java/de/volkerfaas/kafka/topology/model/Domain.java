package de.volkerfaas.kafka.topology.model;

import de.volkerfaas.kafka.topology.validation.ValidVisibilities;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static de.volkerfaas.kafka.topology.ApplicationConfiguration.REGEX_DOMAIN;
import static de.volkerfaas.kafka.topology.ApplicationConfiguration.REGEX_PRINCIPAL;

public class Domain {

    private String name;
    private String description;
    private String principal;
    private Team maintainer;
    private final List<Visibility> visibilities;

    public Domain() {
        this.visibilities = new ArrayList<>();
    }

    public Domain(String name) {
        this();
        this.name = name;
    }

    public Domain(String name, String principal) {
        this();
        this.name = name;
        this.principal = principal;
    }

    // TODO: Validate that domain exists and has a principal
    @NotNull
    @Pattern(
            message = "must be '(country).(company).(domain)'",
            regexp = "^" + REGEX_DOMAIN + "$"
    )
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @NotBlank
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @NotNull
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

    @NotNull
    @Valid
    public Team getMaintainer() {
        return maintainer;
    }

    public void setMaintainer(Team maintainer) {
        this.maintainer = maintainer;
    }

    @Valid
    @ValidVisibilities
    public List<Visibility> getVisibilities() {
        return visibilities;
    }

    @Override
    public String toString() {
        return "Domain{" +
                "name='" + name + "'," +
                "principal='" + principal + "'," +
                "visibilities=[" + this.visibilities.stream().map(Visibility::toString).collect(Collectors.joining(",")) + "]" +
                '}';
    }

}
