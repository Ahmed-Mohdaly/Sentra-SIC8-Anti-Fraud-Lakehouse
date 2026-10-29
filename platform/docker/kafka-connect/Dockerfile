FROM debezium/connect:2.7.3.Final

USER root

# Install unzip, curl, and jq for connector auto-registration
RUN microdnf install -y unzip curl jq && \
    microdnf clean all

# Install Aiven S3 Sink Connector for Apache Kafka
COPY s3-connector.zip /tmp/s3-connector.zip
RUN mkdir -p /kafka/connect/aiven-kafka-connect-s3 && \
    unzip -q /tmp/s3-connector.zip -d /tmp/s3-connector && \
    cp -r /tmp/s3-connector/*/* /kafka/connect/aiven-kafka-connect-s3/ && \
    rm -rf /tmp/s3-connector /tmp/s3-connector.zip && \
    chown -R kafka:kafka /kafka/connect

COPY --chmod=755 register_connectors.sh /kafka/register_connectors.sh
COPY --chmod=755 docker-entrypoint-wrapper.sh /kafka/docker-entrypoint-wrapper.sh

USER kafka

ENTRYPOINT ["/kafka/docker-entrypoint-wrapper.sh"]
