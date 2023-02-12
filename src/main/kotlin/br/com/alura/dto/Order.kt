package br.com.alura.dto

import java.math.BigDecimal

data class Order(
    val userId: String,
    val orderId: String,
    val orderAmount: BigDecimal
)
