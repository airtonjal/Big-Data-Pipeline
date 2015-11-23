package com.airtonjal.poc.stream.storm

import backtype.storm.metric.api.{MeanReducer, ReducedMetric, CountMetric}
import backtype.storm.task.{TopologyContext, OutputCollector}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.{Fields, Values, Tuple}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.airtonjal.poc.pchr.Events
import org.slf4j.{LoggerFactory, Logger}

/**
 * PCHR Bolt to generate call events and measurements
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
class PCHREventsBolt extends BaseRichBolt {

  @transient private var log : Logger = null
  @transient private var _collector : OutputCollector = null
  @transient private var FIELD_NAME : String = null
  @transient private var eventsCount: CountMetric = null
  @transient private var eventsTime : ReducedMetric  = null

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    FIELD_NAME = "pchr"
    outputFieldsDeclarer.declare(new Fields(FIELD_NAME))
  }

  override def prepare(stormConf: java.util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    val clazz = getClass
    log = LoggerFactory.getLogger(clazz)
    log.info("Preparing " + clazz.getName)

    _collector = collector
    FIELD_NAME = "pchr"
    
    initMetrics(context)
  }

  def initMetrics(context: TopologyContext): Unit = {
    eventsCount = new CountMetric()
    eventsTime = new ReducedMetric(new MeanReducer())

    context.registerMetric("events_count", eventsCount, 5)
    context.registerMetric("events_time",  eventsTime, 20)
  }

  override def execute(input: Tuple): Unit = {
    if (input.getFields.contains(FIELD_NAME)) {
      val startTime = System.nanoTime()

      val pchr = input.getValueByField(FIELD_NAME).asInstanceOf[ObjectNode]
      Events.createEvents(pchr)

      updateMetrics(System.nanoTime() - startTime)

      _collector.emit(input, new Values(pchr))
      _collector.ack(input)
    }
  }

  def updateMetrics(duration: Long) : Unit = {
    eventsCount.incr()
    eventsTime.update(duration)
  }

}
