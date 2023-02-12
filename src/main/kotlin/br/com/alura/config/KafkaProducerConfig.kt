package br.com.alura.config

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

class KafkaProducerConfig<T> {

    private val kafkaProducer = KafkaProducer<String, String>(properties())

    fun sendRecord(
        topic: String,
        key: String = UUID.randomUUID().toString(),
        value: T
    ) {
        val stringValue = ObjectMapper().writeValueAsString(value)
        val record = ProducerRecord(topic, key, stringValue)
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

    private fun dataLog(data: RecordMetadata) =
        "${data.topic()} : " +
                "\npartition ${data.partition()} " +
                "\noffset ${data.offset()} " +
                "\ntimestamp ${data.timestamp()}"
}