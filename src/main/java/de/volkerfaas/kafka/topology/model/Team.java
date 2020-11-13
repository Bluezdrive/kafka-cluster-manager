package de.volkerfaas.kafka.topology.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.validation.constraints.Email;
import javax.validation.constraints.NotBlank;

public class Team {

    private String name;
    private String email;

    @NotBlank
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Email
    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    @JsonIgnore
    public String getTeam() {
        return this.name + "<" + this.email + ">";
    }

    @Override
    public String toString() {
        return "Team{" +
                "name='" + name + "\', " +
                "email='" + email + '\'' +
                '}';
    }
}
