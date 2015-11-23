package com.airtonjal.poc.stream.storm

import backtype.storm.metric.api.{MeanReducer, ReducedMetric, CountMetric}
import backtype.storm.task.{TopologyContext, OutputCollector}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.{Fields, Values, Tuple}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.airtonjal.poc.pchr.Geolocation
import org.slf4j.{LoggerFactory, Logger}

/**
 * PCHR Bolt to geolocate call geolocation
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
class PCHRGeolocationBolt extends BaseRichBolt {

  @transient private var log : Logger = null
  @transient private var mapper : ObjectMapper = null
  @transient private var _collector : OutputCollector = null
  @transient private var FIELD_NAME : String = null
  @transient private var geolocationCount: CountMetric = null
  @transient private var geolocationTime : ReducedMetric  = null

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    FIELD_NAME = "pchr"
    outputFieldsDeclarer.declare(new Fields(FIELD_NAME))
  }

  override def prepare(stormConf: java.util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val clazz = getClass
    log = LoggerFactory.getLogger(clazz)
    log.info("Preparing " + clazz.getName)

    _collector = collector
    FIELD_NAME = "pchr"
    
    initMetrics(context)
  }

  def initMetrics(context: TopologyContext): Unit = {
    geolocationCount = new CountMetric()
    geolocationTime = new ReducedMetric(new MeanReducer())

    context.registerMetric("geolocation_count", geolocationCount, 5)
    context.registerMetric("geolocation_time",  geolocationTime, 20)
  }

  override def execute(input: Tuple): Unit = {
    if (input.getFields.contains(FIELD_NAME)) {
      val startTime = System.nanoTime()

      val pchr = input.getValueByField(FIELD_NAME).asInstanceOf[ObjectNode]
      Geolocation.geolocate(pchr)

      updateMetrics(System.nanoTime() - startTime)

      _collector.emit(input, new Values(pchr))
      _collector.ack(input)
    }
  }

  def updateMetrics(duration: Long) : Unit = {
    geolocationCount.incr()
    geolocationTime.update(duration)
  }

}
