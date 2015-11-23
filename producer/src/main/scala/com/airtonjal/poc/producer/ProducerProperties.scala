package com.airtonjal.poc.producer

import java.util.MissingResourceException

import com.airtonjal.poc.ResourceConfiguration

/**
 * Kafka producer {@link java.util.Properties}
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
object ProducerProperties extends ResourceConfiguration {

  val BROKER_PROP      = "metadata.broker.list"
  val SERIALIZER_PROP  = "serializer.class"
  val PARTITIONER_PROP = "partitioner.class"
  val COMPRESSION_PROP = "compression.codec"
  val OFFSET_PROP      = "auto.offset.reset"
  val REQUIRED_PROPS   = Array(BROKER_PROP, SERIALIZER_PROP, PARTITIONER_PROP, COMPRESSION_PROP)

  // Checks if required properties were specified
  for(prop <- REQUIRED_PROPS)
    if (!this.containsKey(prop))
      throw new MissingResourceException("Missing property " + prop + " from properties file", getClass().getName, prop)

  override protected def getFile(): String = "producer.properties"

  def broker = this.containsKey(BROKER_PROP) match {
    case false => throw new MissingResourceException(BROKER_PROP + " property not set in " + getFile(), getClass().getName, BROKER_PROP)
    case true => this.getProperty(BROKER_PROP)
  }
}
