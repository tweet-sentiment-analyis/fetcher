FROM frolvlad/alpine-oraclejdk8:slim

VOLUME /tmp

ADD target/fetcher-0.0.2.jar app.jar

RUN sh -c 'touch /app.jar'

ENV JAVA_OPTS="-Xms512m -Xmx768m"
ENV SQS_QUEUE_NAME="fetched-tweets"

EXPOSE 8080
ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /app.jar" ]