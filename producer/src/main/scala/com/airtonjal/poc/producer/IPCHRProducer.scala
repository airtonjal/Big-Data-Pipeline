package com.airtonjal.poc.producer

import com.airtonjal.poc.CallHistoryRecord

/**
 * PCHR producer trait
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
trait IPCHRProducer {

  /**
   * Producer implementation, sends a pchr contents to Apache Kafka
   * @param pchrs The list of calls to be sent
   */
  def produce(pchrs: List[CallHistoryRecord]): Unit

}
