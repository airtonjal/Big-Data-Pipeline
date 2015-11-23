package com.airtonjal.poc.dispatcher

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.airtonjal.poc.consumer.kafka.impl.PCHRConsumer
import com.airtonjal.poc.stream.storm.SampleTopology
import com.airtonjal.poc.json.JsonUtils
import com.airtonjal.poc.parser.pchr.PCHRParser
import com.airtonjal.poc.pchr.{Geolocation, Enrich, Events, Normalizer}
import com.airtonjal.poc.producer.impl.PCHRProducer
import com.airtonjal.poc.utils.CommandLineUtils
import org.apache.commons.logging._
import scala.collection.JavaConversions._

/**
 * Class to dispatch files to a producer
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
object FileDispatcher {
  private val log = LogFactory.getLog(getClass())

  val PCHR_TOPIC = "pchr"

  /**
   * Usages: java -jar JAR consumer
   *         java -jar JAR producer /path/to/files/to/produce
   *         java -jar JAR spout
   */
  def main(args: Array[String]): Unit = {
    log.info("Starting POC pipeline")

    if (args.length == 0) throw new IllegalArgumentException("A path with files should be provided")

    args(0) match {
      case "consumer" => new PCHRConsumer(PCHR_TOPIC).consume()
      case "topology" => new PCHRTopology(PCHR_TOPIC).start(false)
      case "producer" => {
        val files = CommandLineUtils.getFiles(args(1))
        val producer = new PCHRProducer(PCHR_TOPIC)
        val parser = new PCHRParser()
        if (files != null)
          files.foreach { file =>
            producer.produce(parser.parseList(file).toList)
          }
      }
      case "test" => testClassify(args(1))
    }
  }

  def testClassify(path: String): Unit = {
    val mapper = new ObjectMapper

    val pchrs = new PCHRParser().parseList(new File(path))
    log.info("Finished parsing")

    // Times for instrumentation
    var normalizerTotal  = 0l
    var eventsTotal      = 0l
    var enrichTotal      = 0l
    var geolocationTotal = 0l

    log.info("Processing pchrs")

    var phoneFound = 0
    var tacFound = 0
    for(pchr <- pchrs) {
//      i = i + 1
//      if (i % 1000 == 0) println("Processing pchr " + i)
      val dataMap = pchr.getDataMap
      val oldJson = mapper.writeValueAsString(dataMap)

      val parsedJson = mapper.readTree(oldJson)

      Normalizer.normalize(parsedJson)
      Events.createEvents(parsedJson)
      Enrich.inject(parsedJson)
      Geolocation.geolocate(parsedJson)

      val newJson = mapper.writeValueAsString(parsedJson)

//      println(oldJson)
      if (JsonUtils.exists(parsedJson, Some("Call.TAC")))
        tacFound = tacFound + 1
      if (JsonUtils.exists(parsedJson, Some("Call.Phone")))
        phoneFound = phoneFound + 1
    }

    log.info("Total pchrs: " + pchrs.size)
    log.info("pchrs with tac: " + tacFound)
    log.info("pchrs with phone info: " + phoneFound)

    log.info("Finished")
  }

  def instrumentedClassify(path: String): Unit = {
    val mapper = new ObjectMapper

    val pchrs = new PCHRParser().parseList(new File(path))
    log.info("Finished parsing")

    // Times for instrumentation
    var normalizerTotal  = 0l
    var eventsTotal      = 0l
    var enrichTotal      = 0l
    var geolocationTotal = 0l

    log.info("Converting pchrs")
    // Converts to String and back to acceptable JsonNode format
    val jsons = pchrs.map(pchr => mapper.readTree(mapper.writeValueAsString(pchr.getDataMap)))

    log.info("Processing pchrs")
    val normalizerStartTime = System.currentTimeMillis()
    jsons.foreach(pchr => Normalizer.normalize(pchr))
    normalizerTotal = normalizerTotal + (System.currentTimeMillis() - normalizerStartTime)
    log.info("Normalization took\t\t" + (normalizerTotal.toFloat / 1000) + " seconds")
//    log.info("\tcopy time\t\t" + (Normalizer.copyTime.toFloat / 1000) + " seconds")
//    log.info("\trest time\t\t" + (Normalizer.restTime.toFloat / 1000) + " seconds")
//    log.info("\t\tcopy json time\t\t" + (JsonUtils.copyJsonTime.toFloat / 1000) + " seconds")
//    log.info("\t\tcopy find time\t\t" + (JsonUtils.copyFindTime.toFloat / 1000) + " seconds")
//    log.info("\t\tput obj time\t\t" + (JsonUtils.putObjTime.toFloat / 1000) + " seconds")

    val eventsStartTime = System.currentTimeMillis()
    jsons.foreach(pchr => Events.createEvents(pchr))
    eventsTotal = eventsTotal + (System.currentTimeMillis() - eventsStartTime)
    log.info("Events creation took\t" + (eventsTotal.toFloat / 1000) + " seconds")

    val enrichStartTime = System.currentTimeMillis()
    jsons.foreach(pchr => Enrich.inject(pchr))
    enrichTotal = enrichTotal + (System.currentTimeMillis() - enrichStartTime)
    log.info("Enrichment took\t\t\t" + (enrichTotal.toFloat / 1000) + " seconds")

    val geolocationStartTime = System.currentTimeMillis()
    jsons.foreach(pchr => Geolocation.geolocate(pchr))
    geolocationTotal = geolocationTotal + (System.currentTimeMillis() - geolocationStartTime)
    log.info("Geolocation took\t\t" + (geolocationTotal.toFloat / 1000 ) + " seconds")

//    jsons.foreach(pchr => println(mapper.writeValueAsString(pchr)))
  }



}
