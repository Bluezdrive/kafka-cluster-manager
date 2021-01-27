FROM openjdk:15
ARG JAR_FILE
COPY target/${JAR_FILE} /home/kafka-cluster-manager.jar
WORKDIR /home
ENTRYPOINT ["java", "-jar", "kafka-cluster-manager.jar"]