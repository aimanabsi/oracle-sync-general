FROM confluentinc/cp-kafka-connect:7.5.0

USER root

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-replicator:6.2.0

USER confluent
