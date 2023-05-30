# MQTT-to-Kafka
## idea behind this
To have a microservice running on the edge that subscribes to an existing MQTT broker and routes messages
based on topic to a Kafka instance. Its intended to be highly performant with a tiny footprint and to run as a container.

### ENV Vars to manage the service as a container
```code
ENV TOPIC=/test/topic1
ENV QOS=0
ENV BROKER=192.168.1.30
ENV KAFKA_URL=localhost:9092,localhost:9093,localhost:9094
ENV KAFKA_TOPIC=/test/topic1
ENV KAFKA_PARTITION=0
ENV PORT=1883
ENV USER=mqtt-user
ENV PASS=mqtt-pass
ENV CLIENTID=subscriber-x
ENV WRITESTDOUT=true
```



### TODO
- ADD Kafka and MQTT TLS support
- needs some testing