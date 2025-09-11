FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src src
COPY export_env.sh .

RUN chmod +x ./export_env.sh

RUN mvn package

FROM openjdk:17-jdk-slim

COPY export_env.sh /app/export_env.sh

RUN . /app/export_env.sh

COPY --from=build /app/target/*.jar /high-load-course.jar

CMD ["sh", "-c", ". /app/export_env.sh && java -jar /high-load-course.jar"]
