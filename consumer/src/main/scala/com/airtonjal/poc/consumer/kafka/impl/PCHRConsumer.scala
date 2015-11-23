package com.airtonjal.poc.consumer.kafka.impl

import java.io.PrintStream
import java.util.Random

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.airtonjal.poc.consumer.kafka.{ConsumerProperties, IPCHRConsumer}
import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}
import kafka.serializer._
import kafka.utils._
import org.apache.commons.logging.LogFactory

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

/**
 * Consumer that dumps messages out to standard out.
 * @author <a href="mailto:airtonjal@gmail.comg">Airton Libório</a>
 */
class PCHRConsumer(topic: String) extends IPCHRConsumer {

  private val log = LogFactory.getLog(getClass())

  // Print properties
  private val printKey = false
  private val out: PrintStream = System.out
  private val keySeparator = "\t".getBytes
  private val lineSeparator = "\n".getBytes
  private val nullStr = "null".getBytes

  // Randomly chooses a groupId name to avoid polluting zookeeper data
  val groupId = getClass().getName + "" + new Random().nextInt(100000)

  ConsumerProperties.groupId_(groupId)
  val zookeeper = ConsumerProperties.zookeeper
  val fromBeginning = ConsumerProperties.getProperty("auto.offset.reset") == "smallest"

  val config = new ConsumerConfig(ConsumerProperties)
  val connector = Consumer.create(config)

  // Shutdown hook, for gracefully disconnecting from Zookeeper
  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run() {
      connector.shutdown()
      // if there is no group specified then avoid polluting zookeeper with persistent group data, this is a hack
      ZkUtils.maybeDeletePath(zookeeper, "/consumers/" + groupId)
    }
  })

  // Indicates how many messages must go through before logging
  val outputOffset = 1000

  def consume() {
    if(fromBeginning)
      ZkUtils.maybeDeletePath(zookeeper, "/consumers/" + groupId)

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    try {
      val stream = connector.createMessageStreamsByFilter(new Whitelist(topic), 1, new DefaultDecoder(), new DefaultDecoder()).get(0)

      var totalMessages = 0L
      for(messageAndTopic <- stream) {

        val key     = messageAndTopic.key()
        val message = messageAndTopic.message()

        val node = mapper.readTree(message)
        val ci = node.findPath("CommonInfo/CommGenInfo")
//        ci match {
//          case _ => a = ci
//          case None => println("none")
//        }

        if (totalMessages % outputOffset == 0) log.info(totalMessages + " messages were read so far. Message:\n" )

//        if(printKey) {
//          out.write(if (key == null) nullStr else key)
//          out.write(keySeparator)
//        }
//        out.write(if (message == null) nullStr else message)
//        out.write(lineSeparator)

        totalMessages += 1
      }
      out.println("Consumed %d messages".format(totalMessages))

    } catch {
      case NonFatal(e) => log.error("Error processing message, stopping consumer: ", e)
    } finally {
      out.flush()
      connector.shutdown()
    }
  }


}

