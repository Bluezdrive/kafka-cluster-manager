# Kafka Cluster Manager

[![Build Status](https://travis-ci.com/Bluezdrive/kafka-cluster-manager.svg?branch=master)](https://travis-ci.com/Bluezdrive/kafka-cluster-manager)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/6c0e89ca8a24423db9e3f60d9d5c4019)](https://www.codacy.com/gh/Bluezdrive/kafka-cluster-manager/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=Bluezdrive/kafka-cluster-manager&amp;utm_campaign=Badge_Grade)
[![Codacy Badge](https://app.codacy.com/project/badge/Coverage/6c0e89ca8a24423db9e3f60d9d5c4019)](https://www.codacy.com/gh/Bluezdrive/kafka-cluster-manager/dashboard?utm_source=github.com&utm_medium=referral&utm_content=Bluezdrive/kafka-cluster-manager&utm_campaign=Badge_Coverage)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://gitHub.com/Bluezdrive/kafka-cluster-manager/graphs/commit-activity)
[![GPLv3 License](https://img.shields.io/badge/License-GPL%20v3-yellow.svg)](https://opensource.org/licenses/)
[![GitHub release](https://img.shields.io/github/release/Bluezdrive/kafka-cluster-manager.svg)](https://gitHub.com/Bluezdrive/kafka-cluster-manager/releases/)
[![Docker Pulls](https://img.shields.io/docker/pulls/bluezdrive/kafka-cluster-manager)](https://hub.docker.com/repository/docker/bluezdrive/kafka-cluster-manager)
[![Docker Image Size (latest by date)](https://img.shields.io/docker/image-size/bluezdrive/kafka-cluster-manager)](https://hub.docker.com/repository/docker/bluezdrive/kafka-cluster-manager)

The Kafka Cluster Manager supports you in generating a topology from a YAML configuration file in a domain-driven architecture. The tool follows a strict governance, managing the topics within a domain in conjunction with corresponding access control lists.

## Table of Contents
-   [Governance](#governance)
-   [Usage](#usage)
-   [Directory Structure Example](#directory-structure-example)
-   [Topology File Example](#topology-file-example)
-   [Environment Variables](#environment-variables)
-   [Avro Schema Files](#avro-schema-files)
-   [Access Control List Entries](#access-control-list-entries)
-   [Docker Image](#docker-image)
-   [Kafka Cluster Topology Example](https://github.com/Bluezdrive/kafka-cluster-topology)
-   [Change History](#change-history)

## Governance
Each domain has its own service account. Only the domain service account has read and write access to all topics within the domain. Other domains can only get read access to topics either at visibility level or topic level.

## Usage
```text
java -jar /kafka-cluster-manager.jar

Available Flags:
  --help                     Show help.
  --directory=[directory]    Set base directory for topology files. Default is "topology".
  --domain=[domain]          Processes only a single domain
  --cluster=[cluster]        Sets the cluster and uses conf/[cluster].properties or environment variables as configuration 
  --allow-delete-acl         Allow deletion of orphaned ACLs. Cannot be used in combination with flag --domain. (DO NOT USE IN PRODUCTION!)
  --allow-delete-topics      Allow deletion of orphaned topics. Cannot be used in combination with flag --domain. (DO NOT USE IN PRODUCTION!)
  --dry-run                  Makes no changes to the remote topology
  --write-documentation-file Writes the documentation file "topology.md" to the topology directory
  --restore                  Restores the domains listed with flag --domain into file "topology-[domain].yaml"
```

## Directory Structure Example
```text
config
 +---development.yaml
 +---staging.yaml
 +---production.yaml
topology
 +---config
 |    
 +---events
 |    +---de.volkerfaas.arc
 |    |    +---de.volkerfaas.arc.public.user_updated-key.avsc
 |    |    +---de.volkerfaas.arc.public.user_updated-value.avsc
 +---topology-de.volkerfaas.arc.yaml
 +---restore-de.volkerfaas.test.yaml
 +---topology.md
```

| Directory/File                                          | Description                                                                   |
| ------------------------------------------------------- | ----------------------------------------------------------------------------- |
| topology                                                | Root directory for topology                                                   |
| conf                                                    | Directory for configuration files of clusters                                 |
| conf/[cluster].properties                               | Configuration file for cluster [cluster]                                               |
| topology/events                                         | Directory for storing AVRO schemas of events                                  |
| topology-[domain-name].yaml                             | Topology file for domain [domain-name]                                        |
| restore-[domain-name].yaml                              | Restore file for domain [domain-name]                                         |
| topology.md                                             | Markdown file containing the documentation of entire topology                 |
| [domain-name].[visibility-type].[topic.name]-key.avsc   | AVRO key schema file for topic [domain-name].[visibility-type].[topic.name]   |
| [domain-name].[visibility-type].[topic.name]-value.avsc | AVRO value schema file for topic [domain-name].[visibility-type].[topic.name] |

## Topology File Example
```YAML
domain:
  name: "de.volkerfaas.arc"
  description: "Test domain for architecture stuff"
  principal: "User:129849"
  maintainer:
    name: "Volker Faas"
    email: "bluezdrive@volkerfaas.de"
  visibilities:
    - type: public
      consumers:
        - principal: "User:125382"
      topics:
        - name: "user_updated" # Full topic name is "de.volkerfaas.arc.public.user_updated"
          version: 1
          description: "Dummy topic for architecture stuff."
          clusters: ['development', 'staging']
          numPartitions: 3
          replicationFactor: 3
          keySchema:
            file: "events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-key.avsc"
          valueSchema:
            file: "events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc"
          config:
            cleanupPolicy: "compact"
```

| Field                                                        | Mandatory | Value                          | Description                                                                                                      |
| ------------------------------------------------------------ | ----------| ------------------------------ |----------------------------------------------------------------------------------------------------------------- |
| domain.name                                                  | Yes       | ^([a-z]+)\.([a-z]+)\.([a-z]+)$ | Name of the domain described by this topology file                                                               |
| domain.description                                           | Yes       | string                         | Short description of what the domain stands for                                                                  |
| domain.principal                                             | Yes       | ^(User)+\:([0-9]+)*$           | Reference to service account for accessing topics at domain level in format "User:[service-account-id]"          |
| domain.maintainer.name                                       | Yes       | string                         | Name of person or team that maintains the domain                                                                 |
| domain.maintainer.email                                      | Yes       | email                          | E-Mail-Address of person or team that maintains the domain                                                       |
| domain.visibilities[].type                                   | Yes       | public, protected or private   | Topic visibility as one of public, protected or private                                                          |
| domain.visibilities[].consumers[].principal                  | No        | ^(User)+\:([0-9]+)*$           | Reference to service account for accessing topics at visibility level in format "User:[service-account-id]"      |
| domain.visibilities[].topics[].name                          | Yes       | ^[a-z]+(_[a-z]+)*$             | Name of topic                                                                                                    |
| domain.visibilities[].topics[].version                       | No        | numeric                        | Version of topic                                                                                                 |
| domain.visibilities[].topics[].description                   | Yes       | string                         | Short description of what kind of events the topic handles                                                       |
| domain.visibilities[].topics[].clusters                      | No        | string                         | Clusters to deploy that topic. Matches to the cluster selected by the --cluster option                           |
| domain.visibilities[].topics[].numPartitions                 | Yes       | 1 - 20 (default 6)             | Number of partitions to be created for topic                                                                     |
| domain.visibilities[].topics[].replicationFactor             | No        | short (default 3)              | Number of partitions to be created for topic                                                                     |
| domain.visibilities[].topics[].keySchema.file                | No        | path                           | Relative path to the key schema file associated with the topic                                                   |
| domain.visibilities[].topics[].keySchema.type                | No        | AVRO, PROTOBUF or JSON         | Type of key schema                                                                                               |
| domain.visibilities[].topics[].keySchema.compatibilityMode   | No        | FORWARD_TRANSITIVE, FULL, etc. | Compatibility mode of key schema                                                                                 |
| domain.visibilities[].topics[].valueSchema.file              | Yes       | path                           | Relative path to the value schema file associated with the topic                                                 |
| domain.visibilities[].topics[].valueSchema.type              | No        | AVRO, PROTOBUF or JSON         | Type of value schema                                                                                             |
| domain.visibilities[].topics[].valueSchema.compatibilityMode | No        | FORWARD_TRANSITIVE, FULL, etc. | Compatibility mode of value schema                                                                               |
| domain.visibilities[].topics[].config                        | No        | key/value map                  | Configuration parameters in camelCase for topic                                                                  |
| domain.visibilities[].topics[].consumers[].principal         | No        | ^(User)+\:([0-9]+)*$           | Reference to service account for accessing topics at topic level in format "User:[service-account-id]"           |

## Environment Variables
The variables can be used as environment variables as well as in a YAML property file.
| Variable                   | Description                                             |
| -------------------------  | ------------------------------------------------------- |
| BOOTSTRAP_SERVER           | Bootstrap server of Apache Kafka® cluster to connect to |
| CLUSTER_API_KEY            | API key for accessing cluster                           |
| CLUSTER_API_SECRET         | API secret for accessing cluster                        |
| SCHEMA_REGISTRY_URL        | URL to schema registry associated with cluster          |
| SCHEMA_REGISTRY_API_KEY    | API key for accessing the schema registry               |
| SCHEMA_REGISTRY_API_SECRET | API secret for accessing the schema registry            |

### YAML Property file
```YAML
BOOTSTRAP_SERVER: localhost:9092
CLUSTER_API_KEY: ABCDE1F2GH3IJKL4
CLUSTER_API_SECRET: ABcdE1F2345/6789gh0I6G7ZYX1qHDJ0BNMUZ3kl+6n7klvPtIZ1XpDnlgC+guv0
SCHEMA_REGISTRY_URL: https://localhost:8080
SCHEMA_REGISTRY_API_KEY: MNOPQ5R6ST7UVWX8
SCHEMA_REGISTRY_API_SECRET: HGcdE1F2345/6789as0I6G7ZYX1qBHS0BNMUZ3kl+6n7klvPtIZ1XpDnlgC/mnb3
```

## Avro Schema Files
Avro Schema files will always be stored in the subdirectory "events" in the respective directory for the domain.

## Access Control List Entries
In order to access an Apache Kafka® cluster with the Kafka Cluster Manager, the following permissions must be set for a service account so that the corresponding operations can be performed.

| Permission | Operation        | Resource | Name          | Type    |
| ---------- | ---------------- | -------- | ------------- | ------- |
| ALLOW      | CREATE           | CLUSTER  | kafka-cluster | LITERAL |
| ALLOW      | ALTER            | CLUSTER  | kafka-cluster | LITERAL |
| ALLOW      | DESCRIBE         | CLUSTER  | kafka-cluster | LITERAL |
| ALLOW      | ALTER            | TOPIC    | *             | LITERAL |
| ALLOW      | DELETE           | TOPIC    | *             | LITERAL |
| ALLOW      | ALTER_CONFIGS    | TOPIC    | *             | LITERAL |
| ALLOW      | DESCRIBE         | TOPIC    | *             | LITERAL |
| ALLOW      | DESCRIBE_CONFIGS | TOPIC    | *             | LITERAL |
| ALLOW      | DESCRIBE         | GROUP    | *             | LITERAL |

### Consumer and Producer Principal at Domain Level
The Kafka Cluster Manager sets the following ACL entries when a principal at domain level: 

| Permission | Operation        | Resource         | Name           | Type     |
| ---------- | ---------------- | ---------------- | -------------- | -------- |
| ALLOW      | DESCRIBE         | TOPIC            | [domain-name]. | PREFIXED |
| ALLOW      | READ             | TOPIC            | [domain-name]. | PREFIXED |
| ALLOW      | WRITE            | TOPIC            | [domain-name]. | PREFIXED |
| ALLOW      | WRITE            | TRANSACTIONAL_ID | [domain-name]. | PREFIXED |
| ALLOW      | IDEMPOTENT_WRITE | CLUSTER          | kafka-cluster  | LITERAL  |
| ALLOW      | READ             | GROUP            | [domain-name]. | PREFIXED |

### Consumer Principal at Visibility Level
The Kafka Cluster Manager sets the following ACL entries when a principal at visibility level:

| Permission | Operation        | Resource         | Name                             | Type     |
| ---------- | ---------------- | ---------------- | -------------------------------- | -------- |
| ALLOW      | READ             | GROUP            | [domain-name].[visibility-type]. | PREFIXED |
| ALLOW      | READ             | TOPIC            | [domain-name].[visibility-type]. | PREFIXED |
| ALLOW      | DESCRIBE         | TOPIC            | [domain-name].[visibility-type]. | PREFIXED |

### Consumer Principal at Topic Level
The Kafka Cluster Manager sets the following ACL entries when a principal at topic level:

| Permission | Operation        | Resource         | Name                                         | Type    |
| ---------- | ---------------- | ---------------- | -------------------------------------------- | ------- |
| ALLOW      | READ             | GROUP            | [domain-name].[visibility-type].[topic-name] | LITERAL |
| ALLOW      | READ             | TOPIC            | [domain-name].[visibility-type].[topic-name] | LITERAL |
| ALLOW      | DESCRIBE         | TOPIC            | [domain-name].[visibility-type].[topic-name] | LITERAL |

## Docker Image
The Kafka Cluster Manager is available as docker image from docker hub. Below you find a shell script that can be used to run the docker image.
```shell script
#!/bin/sh
docker run --name kafka-cluster-manager -v "$(pwd)/topology:/home/topology" -v "$(pwd)/conf:/home/conf" bluezdrive/kafka-cluster-manager:2.0 "$@"
docker rm /kafka-cluster-manager > /dev/null 2>&1
``` 

## Change History

### 2.2

-   Added validation for properties
-   Flag cluster is mandatory by now
-   Support for topic version

### 2.1

-   Validation of command line arguments

### 2.0.2

-   Fixed: Orphaned ACLs not printed in dry run.


### 2.0.1

-   Fixed: Get version of a not existing schema threw an exception.

### 2.0

-   Moved schema definition into own object for future extension.
-   Support for schema types Protobuf and JSON besides AVRO.
-   Support for compatibility mode of schema
-   Improved output
-   Removed field "cluster" and flag "--cluster=[cluster]" in favour of flag "--config=[config]"

### 1.1

-   Assign topics to clusters with topic field "cluster" and flag "--cluster=[cluster]"

### 1.0

-   Create topics
-   Increase partitions
-   Alter Configuration of a topic
-   Restore topology of a domain from the cluster
-   Support for multiple environments
-   Remove orphaned access control list entries at visibility and topic level (do not use in production)