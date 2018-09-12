FROM wurstmeister/kafka:latest
ENV mec-poc=192.168.1.61
ENV topic_key=video-file-name
RUN mkdir /kafka-cli
RUN cd /kafka-cli
WORKDIR /kafka-cli
COPY . .
RUN chmod 777 /kafka-cli/create_kafka_topic.sh
ENV CUSTOM_INIT_SCRIPT=/kafka-cli/create_kafka_topic.sh
