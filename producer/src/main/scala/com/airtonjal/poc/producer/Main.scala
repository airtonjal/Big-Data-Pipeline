package com.airtonjal.poc.producer

import java.text.{SimpleDateFormat, DateFormat}
import java.util.{Locale, TimeZone, Date}

import com.airtonjal.poc.parser.pchr.PCHRParser
import com.airtonjal.poc.producer.impl.PCHRProducer
import com.airtonjal.poc.utils.{SizeFormatter, CommandLineUtils}
import org.apache.commons.logging.LogFactory
import scala.collection.JavaConversions._
import scala.concurrent.Future

/**
 * Producer entry point
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
object Main {

  private val log = LogFactory.getLog(getClass())

  val PCHR_TOPIC = "pchr"

  def main(args: Array[String]): Unit = {
    log.info("Starting POC pipeline")

    val files = CommandLineUtils.getFiles(args(0))

    if (files != null) {
      val pchrParser = new PCHRParser
      val producer = new PCHRProducer(PCHR_TOPIC)

      var parserTime    = 0l
      var totalVolume   = 0l
      var numberOfFiles = 0l

      val timeFormat: DateFormat = new SimpleDateFormat("HH:mm:ss", Locale.getDefault)
      var producerTime     = 0l
      var numberOfMessages = 0l
      var sent             = 0

      files foreach { file =>
        val parserStartTime = System.currentTimeMillis()

        val pchrs = pchrParser.parseList(file)

        numberOfFiles = numberOfFiles + 1
        totalVolume = totalVolume + file.length()
        log.info("File " + file.getName() + " parsed\tProducing to Kafka")
        parserTime = parserTime + System.currentTimeMillis() - parserStartTime

        log.info("PCHR Parsing throughput: " + SizeFormatter.readableFileSize((totalVolume / (parserTime / 1000f)).toInt) +
          " per second\t\t\tTotal parsed: " + SizeFormatter.readableFileSize(totalVolume) + "\t\tTotal parser time: " + timeFormat.format(new Date(parserTime - TimeZone.getDefault.getRawOffset)))


        val producerStartTime = System.currentTimeMillis()

        producer.produce(pchrs.toList)

        numberOfMessages = numberOfMessages + pchrs.size()
        producerTime = producerTime + System.currentTimeMillis() - producerStartTime

        log.info("PCHR Producer throughput: " + (numberOfMessages / (producerTime / 1000f)).toInt +
          " messages per second\t\t\tTotal produced: " + numberOfMessages + " messages\t\tTotal producer time: " + timeFormat.format(new Date(producerTime - TimeZone.getDefault.getRawOffset)))
      }
    }
  }

}
