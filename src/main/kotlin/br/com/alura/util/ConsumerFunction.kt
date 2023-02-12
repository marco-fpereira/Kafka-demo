package br.com.alura.util

import org.apache.kafka.clients.consumer.ConsumerRecord

interface ConsumerFunction<T> {
    fun consume(record: ConsumerRecord<String, T>)
}
