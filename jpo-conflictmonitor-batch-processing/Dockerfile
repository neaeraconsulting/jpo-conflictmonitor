# ---- Build stage ----
FROM eclipse-temurin:21-jdk AS build
WORKDIR /home

ARG MAVEN_GITHUB_TOKEN_NAME
ARG MAVEN_GITHUB_TOKEN
ARG MAVEN_GITHUB_ORG

ENV MAVEN_GITHUB_TOKEN_NAME=$MAVEN_GITHUB_TOKEN_NAME
ENV MAVEN_GITHUB_TOKEN=$MAVEN_GITHUB_TOKEN
ENV MAVEN_GITHUB_ORG=$MAVEN_GITHUB_ORG

COPY ./pom.xml      .
COPY ./settings.xml .
COPY ./src          ./src/
COPY ./mvnw         .
COPY ./.mvn         ./.mvn/

# Cache dependencies
RUN ./mvnw -s settings.xml dependency:go-offline -B

# Build
RUN ./mvnw -s settings.xml package -DskipTests -B

# ---- Runtime stage ----
FROM eclipse-temurin:21-jre
WORKDIR /home

COPY --from=build /home/target/jpo-conflictmonitor-batch-processing-*.jar jpo-conflictmonitor-batch-processing.jar

ENTRYPOINT ["java", \
    "-jar", \
    "-XX:+ExitOnOutOfMemoryError", \
    "/home/jpo-conflictmonitor-batch-processing.jar"]