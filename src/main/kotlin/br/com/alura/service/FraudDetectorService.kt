package br.com.alura.service

import br.com.alura.config.KafkaConsumerConfig
import br.com.alura.dto.Order
import org.apache.kafka.clients.consumer.ConsumerRecord

private const val ECOMMERCE_TOPIC = "ECOMMERCE_NEW_ORDER"
private const val GROUP_ID = "FRAUD_DETECTOR_SERVICE"

class FraudDetectorService {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val fraudDetectorService = FraudDetectorService()
            val kafkaConsumerConfig = KafkaConsumerConfig<Order>(
                topic = ECOMMERCE_TOPIC,
                groupId = GROUP_ID,
                dataLog = fraudDetectorService::dataLog
            )
            kafkaConsumerConfig.run()
        }
    }

    private fun dataLog(data: ConsumerRecord<String, Order>) =

        println(
            "-----------------------------------------" +
                    "\nProcessing new order, checking for fraud!" +
                    "\n${data.topic()} : " +
                    "\n${data.value()}" +
                    "\npartition ${data.partition()} " +
                    "\noffset ${data.offset()} " +
                    "\ntimestamp ${data.timestamp()}" +
                    "\nOrder Processed!" +
                    "\n-----------------------------------------"
        )

}
