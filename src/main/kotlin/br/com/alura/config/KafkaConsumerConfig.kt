package br.com.alura.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*
import kotlin.reflect.KFunction1
private const val GROUP_ID = "FRAUD_DETECTOR_SERVICE"

class KafkaConsumerConfig(
    private val topic: String,
    private val groupId: String,
    val dataLog: KFunction1<ConsumerRecord<String, String>, Unit>
) {

    private lateinit var consumer: KafkaConsumer<String, String>

    fun run() {
        this.consumer = KafkaConsumer<String, String>(properties())
        consumer.subscribe(Collections.singletonList(topic))
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
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1") // PROCESS ONE MESSAGE AT A TIME
        return properties
    }
}