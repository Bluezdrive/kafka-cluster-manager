package de.volkerfaas.kafka.topology.model;

import de.volkerfaas.kafka.topology.validation.ValidVisibilities;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Domain {

    public static final String NAME_REGEX = "^([a-z]+)\\.([a-z]+)\\.([a-z]+)$";

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

    @NotNull
    @Pattern(regexp = NAME_REGEX)
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
    @Pattern(regexp = AccessControl.PRINCIPAL_REGEX)
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
