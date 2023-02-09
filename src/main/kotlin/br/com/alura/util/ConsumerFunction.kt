package br.com.alura.util

import org.apache.kafka.clients.consumer.ConsumerRecord

interface ConsumerFunction {
    fun consume(record: ConsumerRecord<String, String>)
}
