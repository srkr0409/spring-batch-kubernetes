FROM openjdk:10-jre-slim
ARG JAR_FILE=target/batch-processing-complete-0.0.1-SNAPSHOT.jar
ARG FACT_FOLDER=pacts
COPY ${JAR_FILE} app.jar
CMD java -jar /app.jar