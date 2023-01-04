
List the topic

    kafka-topics.sh --bootstrap-server localhost:9092 --list  

Create the topic

    kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic wikimedia.recentchange --partitions 3 --replication-factor 1

Consume the topic

    kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic wikimedia.recentchange 