package br.com.alura.service

import br.com.alura.config.KafkaConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord

private const val ECOMMERCE_TOPIC = "ECOMMERCE_NEW_ORDER"
private const val GROUP_ID = "FRAUD_DETECTOR_SERVICE"

class FraudDetectorService {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val fraudDetectorService = FraudDetectorService()
            val kafkaConsumerConfig = KafkaConsumerConfig(ECOMMERCE_TOPIC, GROUP_ID, fraudDetectorService::dataLog)
            kafkaConsumerConfig.run()
        }
    }

    private fun dataLog(data: ConsumerRecord<String, String>) =
        println(
            "-----------------------------------------" +
                    "\nProcessing new order, checking for fraud!" +
                    "\n${data.topic()} : " +
                    "\npartition ${data.partition()} " +
                    "\noffset ${data.offset()} " +
                    "\ntimestamp ${data.timestamp()}" +
                    "\nOrder Processed!" +
                    "\n-----------------------------------------"
        )

}
