package com.airtonjal.poc.stream.storm

import backtype.storm.metric.api.{MeanReducer, ReducedMetric, MultiCountMetric, CountMetric}
import backtype.storm.task.{TopologyContext, OutputCollector}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.{Fields, Values, Tuple}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.airtonjal.poc.pchr.Normalizer
import org.slf4j.{LoggerFactory, Logger}

/**
 * Test bolt to normalize pchr messages
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
class PCHRNormalizerBolt extends BaseRichBolt {

  @transient private var log : Logger = null
  @transient private var mapper : ObjectMapper = null
  @transient private var _collector : OutputCollector = null
  @transient private var FIELD_NAME : String = null
  @transient private var normalizerCount: CountMetric = null
  @transient private var normalizerTime : ReducedMetric  = null

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("pchr"))
  }

  override def prepare(stormConf: java.util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val clazz = getClass
    log = LoggerFactory.getLogger(clazz)
    log.info("Preparing " + clazz.getName)

    _collector = collector
    FIELD_NAME = "str"

    initMetrics(context)
  }

  def initMetrics(context: TopologyContext): Unit = {
    normalizerCount = new CountMetric()
    normalizerTime = new ReducedMetric(new MeanReducer())

    context.registerMetric("normalizer_count", normalizerCount, 5)
    context.registerMetric("normalizer_time",  normalizerTime, 20)
  }

  override def execute(input: Tuple): Unit = {
    if (input.getFields.contains(FIELD_NAME)) {
      val startTime = System.nanoTime()

      // Deserializes string json
      val message = input.getStringByField(FIELD_NAME)
      val pchr = mapper.readTree(message)
      Normalizer.normalize(pchr)

      updateMetrics(System.nanoTime() - startTime)

      //    _collector.emit(input, new Values(mapper.writeValueAsString(pchr)))
      _collector.emit(input, new Values(pchr))
      _collector.ack(input)
    }
  }

  def updateMetrics(duration: Long) : Unit = {
    normalizerCount.incr()
    normalizerTime.update(duration)
  }

}
