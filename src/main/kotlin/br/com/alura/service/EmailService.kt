package br.com.alura.service

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Collections
import java.util.Properties

private const val EMAIL_TOPIC = "ECOMMERCE_SEND_EMAIL"
private const val GROUP_ID = "EMAIL_SERVICE"

fun main() {
    val consumer = KafkaConsumer<String, String>(properties())
    consumer.subscribe(Collections.singletonList(EMAIL_TOPIC))
    while(true){
        val records = consumer.poll(Duration.ofMillis(100L))
        if (!records.isEmpty) for (record in records) dataLog(record)
    }
}

private fun properties(): Properties {
    val properties = Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID)
    return properties
}

private fun dataLog(data: ConsumerRecord<String, String>) =
    println(
        "-----------------------------------------" +
        "\nSending e-mail!" +
        "\n${data.topic()} : " +
        "\npartition ${data.partition()} " +
        "\noffset ${data.offset()} " +
        "\ntimestamp ${data.timestamp()}" +
        "\nEmail sent!" +
        "\n-----------------------------------------"
    )
