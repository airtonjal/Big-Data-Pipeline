package com.airtonjal.poc

import java.io.IOException
import java.util.{MissingResourceException, Properties}

import org.apache.commons.logging.LogFactory

import scala.io.Source

/**
 * Configuration object to handle .properties files that should be provided to the application in the resources dir
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
trait ResourceConfiguration extends Properties {

  /**
   * Gets the filename with the properties
   * @return A [[String]] with the connection file name
   */
  protected def getFile() : String

  private val log = LogFactory.getLog(getClass())

  var loaded = false

  try {
    log.info("Attempting to read " + getFile() + " as stream from resources directory")
    val stream = Source.fromURL(Source.getClass().getResource("/" + getFile()))

    this.load(stream.bufferedReader())

    log info getFile() + " read and loaded"
  } catch {
    case ioe: IOException => throw new MissingResourceException(getFile() + " property file not found in resources dir", getClass.getName, getFile)
  }

}
