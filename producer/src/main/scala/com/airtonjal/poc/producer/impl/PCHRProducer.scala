package com.airtonjal.poc.producer.impl

import com.fasterxml.jackson.databind.ObjectMapper
import com.airtonjal.poc.CallHistoryRecord
import com.airtonjal.poc.producer.{IPCHRProducer, ProducerProperties}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import org.apache.commons.logging.LogFactory

import scala.util.control.NonFatal

/**
 * PCHR events file producer implementation. Produces events for the next component in pipeline
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
class PCHRProducer(topic: String) extends IPCHRProducer {

  private val log = LogFactory.getLog(getClass())
//  private val log = LoggerFactory.getLogger(getClass())

  private val NUMBER_OF_PARTITIONS = 10

  log.info("Connecting to Kafka and Zookeeper")
  val producerConfig = new ProducerConfig(ProducerProperties)
  val kafkaProducer = new Producer[String, String](producerConfig)

  // Shutdown hook, for gracefully disconnecting from Zookeeper
  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run() {
      kafkaProducer.close()
    }
  })

//  /** {@inheritdoc} */
  /**
   * Producer implementation, sends a pchr contents to Apache Kafka
   * @param pchrs The list of calls to be sent
   */
  override def produce(pchrs: List[CallHistoryRecord]): Unit = {
    var i = 0
    val mapper = new ObjectMapper()

    pchrs.foreach(chr => {
      if (i % 1000 == 0) log.trace("Sending "+ i + "th chr event")
      i += 1
      val json = mapper.writeValueAsString(chr.getDataMap())

      val message = new KeyedMessage[String, String](topic, (i % NUMBER_OF_PARTITIONS).toString, json)

      try {
        kafkaProducer.send(message)
      } catch {
        case NonFatal(e) => log.error("Sending a pchr event to kafka failed", e)
      }
    })

  }
}