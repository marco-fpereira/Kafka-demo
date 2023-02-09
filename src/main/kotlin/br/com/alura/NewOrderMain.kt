package br.com.alura

import br.com.alura.config.KafkaProducerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

const val ECOMMERCE_TOPIC = "ECOMMERCE_NEW_ORDER"
const val EMAIL_TOPIC = "ECOMMERCE_SEND_EMAIL"

fun main() {
    val kafkaProducerConfig = KafkaProducerConfig()
    kafkaProducerConfig.sendRecord(topic = ECOMMERCE_TOPIC, value = "456789")
    println()

    kafkaProducerConfig.sendRecord(EMAIL_TOPIC, value = "Your order is being processed!")
}

