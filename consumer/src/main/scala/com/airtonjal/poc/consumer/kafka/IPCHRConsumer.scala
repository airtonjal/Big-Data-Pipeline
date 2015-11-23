package com.airtonjal.poc.consumer.kafka

/**
 * Kakfa PCHR consumer interface
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
trait IPCHRConsumer {

  /**
   * Consumes pchr information from kafka
   */
  def consume()
}
