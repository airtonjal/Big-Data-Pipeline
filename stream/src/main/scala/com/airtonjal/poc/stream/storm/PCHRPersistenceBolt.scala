package com.airtonjal.poc.stream.storm

import java.util.concurrent.{LinkedBlockingQueue, BlockingQueue}

import backtype.storm.Constants
import backtype.storm.metric.api.{MeanReducer, ReducedMetric, CountMetric}
import backtype.storm.task.{TopologyContext, OutputCollector}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.airtonjal.poc.stream.elasticsearch.ElasticsearchConfig
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.slf4j.{LoggerFactory, Logger}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
 * PCHR bolt to persist data in elasticsearch
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
class PCHRPersistenceBolt extends BaseRichBolt {

  @transient private var log : Logger = null
  @transient private var mapper : ObjectMapper = null
  @transient private var _collector : OutputCollector = null
  @transient private var FIELD_NAME : String = null
  @transient private var tupleQueue : BlockingQueue[Tuple] = null
  @transient private var BATCH_SIZE : Int = 100
  @transient private var lastFlush: Long = 0
  @transient private var MIN_BATCH_INTERVAL: Long = 1000
  @transient private var esClient: Client = null
  @transient private var persistenceCount: CountMetric = null
  @transient private var persistenceTime : ReducedMetric = null


  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {}

  override def prepare(stormConf: java.util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    tupleQueue = new LinkedBlockingQueue[Tuple]
    BATCH_SIZE = 300
    MIN_BATCH_INTERVAL = 1000

    val clazz = getClass
    log = LoggerFactory.getLogger(clazz)
    log.info("Preparing " + clazz.getName)

    log.info("Elasticsearch name " + ElasticsearchConfig.name)
    log.info("Elasticsearch host " + ElasticsearchConfig.host)
//
//    println("Elasticsearch name " + ElasticsearchConfig.name)
//    println("Elasticsearch host " + ElasticsearchConfig.host)

    //TODO: UGLY HARDCODING, REFACTOR BITCH!!!
    val settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch_poc").build()
    esClient = new TransportClient(settings)
      .addTransportAddress(new InetSocketTransportAddress("localhost", 9300))

    FIELD_NAME = "pchr"
    _collector = collector

    initMetrics(context)
  }

  def initMetrics(context: TopologyContext): Unit = {
    persistenceCount = new CountMetric()
    persistenceTime = new ReducedMetric(new MeanReducer())

    context.registerMetric("persistence_count", persistenceCount, 5)
    context.registerMetric("persistence_time",  persistenceTime, 20)
  }

  override def execute(input: Tuple): Unit = {
    if (isTickTuple(input)) {
      log.info("Tick tuple")
      // Flush only if previous flush was not done very recently
      val lastBatch = System.currentTimeMillis() - lastFlush
      if (lastBatch >= MIN_BATCH_INTERVAL) {
        log.info("Last batch was done " + lastBatch + "ms back. Received tuple tick and flushing queue of size: "
          + tupleQueue.size)
        flushBatch()
      } else {
        log.info("Current queue size is " + tupleQueue.size() + ". Received tick tuple but last batch was executed "
          + lastBatch + "ms back that is less than " + MIN_BATCH_INTERVAL + " so ignoring the tick tuple")
      }
    } else {
      tupleQueue.add(input)
      _collector.ack(input)
      if (tupleQueue.size >= BATCH_SIZE) {
        log.info("Current queue size reached " + BATCH_SIZE + ". Flushing batch request")
        flushBatch()
      }
    }
  }

  private def flushBatch() : Unit = {
    log.info("Flushing pchr batch of size " + tupleQueue.size)
    if (tupleQueue.size == 0) return

    lastFlush = System.currentTimeMillis()

    val startTime = System.nanoTime()
    val size = tupleQueue.size()

    val tupleList = new ListBuffer[Tuple]
    tupleQueue.drainTo(tupleList)

    val mapper = new ObjectMapper
    val bulkRequest = esClient.prepareBulk()
    tupleList.foreach{tuple =>
      val pchr = tuple.getValueByField(FIELD_NAME).asInstanceOf[ObjectNode]
      bulkRequest.add(esClient.prepareIndex("pchrindex", "pchr").setSource(mapper.writeValueAsString(pchr)))
    }

    try {
      val response = bulkRequest.execute().actionGet()
      for(i <- 0 to response.getItems.length - 1) {
        val resp = response.getItems()(i)
        val tuple = tupleList.get(i)

        if (resp.isFailed) {
          log.warn("A failed pchr insertion into elasticsearch was found")
          tupleQueue.add(tuple)  // Re-enqueue tuple to be inserted later on
          _collector.fail(tuple)
        }
//        else _collector.ack(tuple)
        else
          updateMetrics(System.nanoTime() - startTime, size)

      }
    } catch {
      case e: Exception => {
        log.error(e.getMessage)
        tupleList.foreach(tuple => _collector.fail(tuple))
      }
    }
  }

  def updateMetrics(duration: Long, size: Long) : Unit = {
    persistenceCount.incrBy(size)
    persistenceTime.update(duration / size)
  }


  private def isTickTuple(tuple: Tuple): Boolean =
    tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)


}

