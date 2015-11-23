package com.airtonjal.poc.stream.elasticsearch

import java.util.MissingResourceException

import com.airtonjal.poc.ResourceConfiguration
import org.slf4j.LoggerFactory

/**
 * Elasticsearch configuration
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
object ElasticsearchConfig extends ResourceConfiguration {

  override protected def getFile(): String = "elasticsearch.properties"

  private val log = LoggerFactory.getLogger(getClass())

  private val ES_HOST_PROPERTY = "es.cluster.host"
  private val ES_NAME_PROPERTY = "es.cluster.name"

  def host = this.containsKey(ES_HOST_PROPERTY) match {
    case false => throw new MissingResourceException(ES_HOST_PROPERTY + " property not set in " + getFile(), getClass().getName, ES_HOST_PROPERTY)
    case true  => this.getProperty(ES_HOST_PROPERTY)
  }

  def name = this.containsKey(ES_NAME_PROPERTY) match {
    case false => throw new MissingResourceException(ES_NAME_PROPERTY + " property not set in " + getFile(), getClass().getName, ES_NAME_PROPERTY)
    case true  => this.getProperty(ES_NAME_PROPERTY)
  }

}
