package de.volkerfaas.kafka.topology.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.volkerfaas.kafka.topology.validation.ValidTopologyFile;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.File;

@ValidTopologyFile
public class TopologyFile {

    private File file;
    private Domain domain;

    @NotNull
    @JsonIgnore
    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    @Valid
    public Domain getDomain() {
        return domain;
    }

    public void setDomain(Domain domain) {
        this.domain = domain;
    }

    @Override
    public String toString() {
        return "TopologyFile{" +
                "file='" + file + "'," +
                "domain=" + domain +
                '}';
    }

}
