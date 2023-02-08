package br.com.alura

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

const val TOPIC = "ECOMMERCE_NEW_ORDER"

fun main() {
    val kafkaProducer = KafkaProducer<String, String>(properties())

    val record = ProducerRecord(TOPIC, "123", "456789")
    kafkaProducer.send(record) { data, ex ->
        if(ex != null) {
            ex.printStackTrace()
            return@send
        }
        println(dataLog(data))
    }.get()
}

private fun properties(): Properties {
    val properties = Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    return properties
}

fun dataLog(data: RecordMetadata) =
            "${data.topic()} : " +
            "\npartition ${data.partition()} " +
            "\noffset ${data.offset()} " +
            "\ntimestamp ${data.timestamp()}"
