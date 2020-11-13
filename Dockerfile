FROM openjdk:15
MAINTAINER bluezdrive <bluezdrive@volkerfaas.de>
ARG JAR_FILE
COPY target/${JAR_FILE} /kafka-cluster-manager.jar
WORKDIR /
CMD java -jar /kafka-cluster-manager.jar