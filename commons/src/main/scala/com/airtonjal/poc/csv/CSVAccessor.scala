package com.airtonjal.poc.csv

import java.util.MissingResourceException

import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.control.NonFatal

/**
 * CSV accessor that reads and indexes registers from a file
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
trait CSVAccessor {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Gets the filename of the csv file
   * @return The csv filename
   */
  def getFilename(): String

  /**
   * Gets the separator char
   * @return The separator char
   */
  def getSeparator(): Char

  /** Indicates do scala-csv that separator is actually a semicolon */
  implicit object SemicolonFormat extends DefaultCSVFormat {
    override val delimiter = getSeparator()
  }

  // Parses CSV
  val lines = try {
    log.info("Attempting to read " + getFilename() + " as stream from resources directory")
    val stream = Source.fromURL(Source.getClass().getResource("/" + getFilename()))
    val reader = stream.bufferedReader()

    val csvReader = CSVReader.open(reader)(SemicolonFormat)
    val lines = csvReader.allWithHeaders()
    reader.close()
    log.info(getFilename() + " read and loaded")

    lines
  } catch {
    case NonFatal(e) => throw new MissingResourceException(getFilename() + " property file not found in resources dir", getClass.getName, getFilename)
  }

}
