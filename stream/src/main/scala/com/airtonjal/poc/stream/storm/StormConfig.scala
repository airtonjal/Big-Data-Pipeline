package com.airtonjal.poc.stream.storm

import java.util.MissingResourceException

import backtype.storm.Config
import com.airtonjal.poc.ResourceConfiguration
import org.apache.commons.logging.LogFactory

/**
 * Storm configuration
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
object StormConfig extends ResourceConfiguration {

  override protected def getFile(): String = "storm.properties"

  private val log = LogFactory.getLog(getClass())

  def nimbusHost = this.containsKey(Config.NIMBUS_HOST) match {
    case false => throw new MissingResourceException(Config.NIMBUS_HOST + " property not set in " + getFile(), getClass().getName, Config.NIMBUS_HOST)
    case true  => this.getProperty(Config.NIMBUS_HOST)
  }

  def nimbusPort = this.containsKey(Config.NIMBUS_THRIFT_PORT) match {
    case false => throw new MissingResourceException(Config.NIMBUS_THRIFT_PORT + " property not set in " + getFile(), getClass().getName, Config.NIMBUS_THRIFT_PORT)
    case true  => this.getProperty(Config.NIMBUS_THRIFT_PORT)
  }

  private val ZOOKEEPER_PROPERTY = "zookeeper.connect"

  def zookeeper = this.containsKey(ZOOKEEPER_PROPERTY) match {
    case false => throw new MissingResourceException(ZOOKEEPER_PROPERTY + " property not set in " + getFile(), getClass().getName, ZOOKEEPER_PROPERTY)
    case true => this.getProperty(ZOOKEEPER_PROPERTY)
  }

}
