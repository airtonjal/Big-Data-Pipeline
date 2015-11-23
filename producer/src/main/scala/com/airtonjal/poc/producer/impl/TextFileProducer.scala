package com.airtonjal.poc.producer.impl

import java.io.File
import java.util.Properties

import com.airtonjal.poc.CallHistoryRecord
import com.airtonjal.poc.producer.IPCHRProducer
import kafka.producer.{Producer, ProducerConfig, KeyedMessage}
import org.apache.commons.logging.LogFactory

import scala.io.Source

/**
 * Standard file producer implementation. Produces a text file for the next component in pipeline
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
class TextFileProducer(topic: String, broker: String) extends IPCHRProducer {

  val log = LogFactory.getLog(getClass())

  val props = new Properties()

  props.put("metadata.broker.list", broker)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
  props.put("compression.codec", "2")

  val kafkaConfig = new ProducerConfig(props)
  val kafkaProducer = new Producer[String, String](kafkaConfig)

  /** {@inheritdoc} */
  override def produce(pchrs: List[CallHistoryRecord]) = {
//    log.info("Producing files " + file.getPath)

//    log.info("Reading file " + file.getName() + " contents")
//    val fileContents = Source.fromFile(file).mkString
//    log.info(file.getName() + " contents read")

//    val message = new KeyedMessage[String, String]("pchr", fileContents)

//    log.info("Sending file " + file.getName() + " contents")

//    kafkaProducer.send(message)
  }

}
