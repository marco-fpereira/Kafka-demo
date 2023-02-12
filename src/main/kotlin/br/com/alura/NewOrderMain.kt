package br.com.alura

import br.com.alura.config.KafkaProducerConfig
import br.com.alura.dto.Order
import java.math.BigDecimal
import java.util.*

const val ECOMMERCE_TOPIC = "ECOMMERCE_NEW_ORDER"
const val EMAIL_TOPIC = "ECOMMERCE_SEND_EMAIL"

fun main() {
    val kafkaEcommerceProducerConfig = KafkaProducerConfig<Order>()
    val order = Order(
        userId = UUID.randomUUID().toString(),
        orderId = UUID.randomUUID().toString(),
        orderAmount = BigDecimal.valueOf(Math.random()*200+1)
    )
    kafkaEcommerceProducerConfig.sendRecord(topic = ECOMMERCE_TOPIC, key = order.orderId, value = order)
    println()

    val kafkaEmailProducerConfig = KafkaProducerConfig<String>()
    kafkaEmailProducerConfig.sendRecord(EMAIL_TOPIC, value = "Your order is being processed!")
}

