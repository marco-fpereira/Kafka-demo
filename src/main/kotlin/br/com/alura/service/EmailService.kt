package br.com.alura.service

import br.com.alura.config.KafkaConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord

private const val EMAIL_TOPIC = "ECOMMERCE_SEND_EMAIL"
private const val GROUP_ID = "EMAIL_SERVICE"

class EmailService {

    companion object{
        @JvmStatic
        fun main(args: Array<String>) {
            val emailService = EmailService()
            val kafkaConsumerConfig = KafkaConsumerConfig(EMAIL_TOPIC, GROUP_ID, emailService::dataLog)
            kafkaConsumerConfig.run()
        }
    }

    private fun dataLog(data: ConsumerRecord<String, String>) =
        println(
            "-----------------------------------------" +
                    "\nSending e-mail!" +
                    "\n${data.topic()} : " +
                    "\npartition ${data.partition()} " +
                    "\noffset ${data.offset()} " +
                    "\ntimestamp ${data.timestamp()}" +
                    "\nEmail sent!" +
                    "\n-----------------------------------------"
        )

}
