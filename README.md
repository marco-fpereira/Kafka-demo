# Kafka-demo
Demonstration of a Kotlin project that exemplifies how to send and receive Kafka messages programmatically 

# Commands 
Run Zookeper
	bin/windows/zookeeper-server-start.bat config/zookeeper.properties

Run Kafka
	bin/windows/kafka-server-start.bat config/server.properties

List topics
	bin/windows/kafka-topics.bat --list --bootstrap-server localhost:9092

Create consumer 
	bin/windows/kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic ECOMMERCE_NEW_ORDER --from-beginning
