# Kafka Cluster Manager

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/af4e3d89b0a9459a9dd89789bab0ed38)](https://app.codacy.com/gh/Bluezdrive/kafka-cluster-manager?utm_source=github.com&utm_medium=referral&utm_content=Bluezdrive/kafka-cluster-manager&utm_campaign=Badge_Grade)
[![Build Status](https://travis-ci.org/Bluezdrive/kafka-cluster-manager.svg?branch=master&service=github)](https://travis-ci.org/Bluezdrive/kafka-cluster-manager)
[![Coverage Status](https://coveralls.io/repos/github/Bluezdrive/kafka-cluster-manager/badge.svg?branch=master)](https://coveralls.io/github/Bluezdrive/kafka-cluster-manager?branch=master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/178f2aac0e9b4f69bdb0b9285be5397c)](https://app.codacy.com/gh/Bluezdrive/kafka-cluster-manager?utm_source=github.com&utm_medium=referral&utm_content=Bluezdrive/kafka-cluster-manager&utm_campaign=Badge_Grade_Settings)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://gitHub.com/Bluezdrive/kafka-cluster-manager/graphs/commit-activity)
[![GPLv3 License](https://img.shields.io/badge/License-GPL%20v3-yellow.svg)](https://opensource.org/licenses/)
[![GitHub release](https://img.shields.io/github/release/Bluezdrive/kafka-cluster-manager.svg)](https://gitHub.com/Bluezdrive/kafka-cluster-manager/releases/)
[![Docker Pulls](https://img.shields.io/docker/pulls/bluezdrive/kafka-cluster-manager)](https://hub.docker.com/repository/docker/bluezdrive/kafka-cluster-manager)
[![Docker Image Size (latest by date)](https://img.shields.io/docker/image-size/bluezdrive/kafka-cluster-manager)](https://hub.docker.com/repository/docker/bluezdrive/kafka-cluster-manager)

The Kafka Cluster Manager supports you in generating a topology from a YAML configuration file in a domain-driven architecture. The tool follows a strict governance, managing the topics within a domain in conjunction with corresponding access control lists.

## Governance
Each domain has its own service account. Only the domain service account has read and write access to all topics within the domain. Other domain can only get read access to topics either at visibility level or topic level.

## Usage
```bash
java -jar /kafka-cluster-manager.jar

Available Flags:
  --help                     Show help.
  --directory=[directory]    Set base directory for topology files. Default is "topology".
  --domain=[domain]          Processes only a single domain
  --allow-delete             Allow deletion of orphaned ACLs. Cannot be used in combination with flag --domain. (DO NOT USE IN PRODUCTION!)
  --dry-run                  Makes no changes to the remote topology
  --restore                  Restores the domains listed with flag --domain into file "topology-[domain].yaml"
```

## Directory Structure
```txt
topology
 +---events
 |    +---de.volkerfaas.arc
 |    |    +---de.volkerfaas.arc.public.user_updated-key.avsc
 |    |    +---de.volkerfaas.arc.public.user_updated-value.avsc
 +---topology-de.volkerfaas.arc.yaml
```

## Configuration File
```YAML
domain:
  name: "de.volkerfaas.arc"
  description: "Test domain for architecture stuff"
  principal: "User:129849"
  maintainer:
    team: "Volker Faas"
    email: "bluezdrive@volkerfaas.de"
  visibilities:
    - type: public
      consumers:
        - principal: "User:125382"
      topics:
        - name: "user_updated"
          description: "Dummy topic for architecture stuff."
          numPartitions: 3
          replicationFactor: 3
          keySchemaFile: "events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-key.avsc"
          valueSchemaFile: "events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc"
          config:
            cleanupPolicy: "compact"
```

## Avro Schema files

Avro Schema files will always be stored in the subdirectory "events" in the respective directory for the domain.

## Access Control List Entries

In order to access an Apache Kafka® cluster with the Kafka Cluster Manager, the following permissions must be set for a service account so that the corresponding operations can be performed.

| Permission | Operation        | Resource | Name          | Type    |
| ---------- | ---------------- | -------- | ------------- | ------- |
| ALLOW      | ALTER            | CLUSTER  | kafka-cluster | LITERAL |
| ALLOW      | CREATE           | CLUSTER  | kafka-cluster | LITERAL |
| ALLOW      | DESCRIBE         | CLUSTER  | kafka-cluster | LITERAL |
| ALLOW      | ALTER            | TOPIC    | *             | LITERAL |
| ALLOW      | DESCRIBE_CONFIGS | TOPIC    | *             | LITERAL |
| ALLOW      | DESCRIBE         | TOPIC    | *             | LITERAL |

### Domain Principal

### Consumer Principal

## Change History

### 1.0

*   Create topics
*   Increase partitions
*   Alter Configuration of a topic
*   Restore topology of a domain from the cluster
*   Support for multiple environments
*   Remove orphaned access control list entries at visibility and topic level (do not use in production)