package com.airtonjal.poc.stream

import com.airtonjal.poc.stream.storm.SampleTopology

/**
 * Storm topology submitter
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
object Main {

  val PCHR_TOPIC = "pchr"

  def main(args: Array[String]): Unit = {
    new SampleTopology(PCHR_TOPIC).start(false)
  }
}
