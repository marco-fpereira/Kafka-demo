package br.com.alura.config

import br.com.alura.dto.enums.TopicNamePatternEnum
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*
import kotlin.reflect.KFunction1
import java.util.regex.Pattern

class KafkaConsumerConfig<T>(
    private val topic: String,
    private val topicNamePattern: TopicNamePatternEnum = TopicNamePatternEnum.STRING,
    private val groupId: String,
    val dataLog: KFunction1<ConsumerRecord<String, T>, Unit>
) {

    private lateinit var consumer: KafkaConsumer<String, T>

    fun run() {
        this.consumer = KafkaConsumer<String, T>(properties())
        if (topicNamePattern == TopicNamePatternEnum.REGEX) consumer.subscribe(Pattern.compile(topic))
        else consumer.subscribe(Collections.singletonList(topic))

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