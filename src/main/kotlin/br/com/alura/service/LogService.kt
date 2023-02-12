package br.com.alura.service

import br.com.alura.config.KafkaConsumerConfig
import br.com.alura.dto.enums.TopicNamePatternEnum
import org.apache.kafka.clients.consumer.ConsumerRecord

private const val GROUP_ID = "LOG_SERVICE"

class LogService {

    companion object{
        @JvmStatic
        fun main(args: Array<String>) {
            val logService = LogService()

            val kafkaConsumerConfig = KafkaConsumerConfig(
                topic = "ECOMMERCE.*",
                topicNamePattern = TopicNamePatternEnum.REGEX,
                groupId = GROUP_ID,
                dataLog = logService::dataLog
            )
            kafkaConsumerConfig.run()
        }
    }
    private fun dataLog(data: ConsumerRecord<String, String>) =
        println(
            "-----------------------------------------" +
                    "\n${data.topic()} : " +
                    "\n${data.value()}" +
                    "\npartition ${data.partition()} " +
                    "\noffset ${data.offset()} " +
                    "\ntimestamp ${data.timestamp()}" +
                    "\n-----------------------------------------"
        )

}
