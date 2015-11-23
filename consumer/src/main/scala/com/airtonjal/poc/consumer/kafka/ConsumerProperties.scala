package com.airtonjal.poc.consumer.kafka

import java.util.MissingResourceException

import com.airtonjal.poc.ResourceConfiguration
import kafka.consumer.ConsumerConfig

/**
 * Kafka consumer {@link java.util.Properties}. You must set the groupId for this object to properly work
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
object ConsumerProperties extends ResourceConfiguration {

  override protected def getFile(): String = "consumer.properties"

  val ZOOKEEPER_PROP = "zookeeper.connect"
  val OFFSET_PROP    = "auto.offset.reset"
  val REQUIRED_PROPS = Array(ZOOKEEPER_PROP, OFFSET_PROP)

  // Checks if required properties were specified
  for(prop <- REQUIRED_PROPS) {
    this.containsKey(prop) match {
      case false => throw new IllegalArgumentException("Missing property " + prop + " from properties file")
      case true => None
    }
  }

  // Other properties, not currently set, change if testing needs to be performed
  // TODO: Move these to consumer.properties
  val fetchSizeOpt = (1024 * 1024).toString
  val minFetchBytesOpt = (1).toString
  val maxWaitMsOpt = (100).toString
  val socketBufferSizeOpt = (2 * 1024 * 1024).toString
  val socketTimeoutMsOpt = (ConsumerConfig.SocketTimeout).toString
  val refreshMetadataBackoffMsOpt = (ConsumerConfig.RefreshMetadataBackoffMs).toString
  val consumerTimeoutMsOpt = (-1).toString
  val autoCommitIntervalOpt = (ConsumerConfig.AutoCommitInterval).toString

  this.put("socket.receive.buffer.bytes", socketBufferSizeOpt)
  this.put("socket.timeout.ms", socketTimeoutMsOpt)
  this.put("fetch.message.max.bytes", fetchSizeOpt)
  this.put("fetch.min.bytes", minFetchBytesOpt)
  this.put("fetch.wait.max.ms", maxWaitMsOpt)
  this.put("auto.commit.enable", "true")
  this.put("auto.commit.interval.ms", autoCommitIntervalOpt)
  this.put("consumer.timeout.ms", consumerTimeoutMsOpt)
  this.put("refresh.leader.backoff.ms", refreshMetadataBackoffMsOpt)


  def groupId_ (groupId : String) = this.put("group.id", groupId)

  private val ZOOKEEPER_PROPERTY = "zookeeper.connect"

  def zookeeper = this.containsKey(ZOOKEEPER_PROPERTY) match {
    case false => throw new MissingResourceException(ZOOKEEPER_PROPERTY + " property not set in " + getFile(), getClass().getName, ZOOKEEPER_PROPERTY)
    case true => this.getProperty(ZOOKEEPER_PROPERTY)
  }

}
