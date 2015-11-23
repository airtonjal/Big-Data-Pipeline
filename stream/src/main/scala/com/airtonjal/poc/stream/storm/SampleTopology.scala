package com.airtonjal.poc.stream.storm

import backtype.storm.metric.LoggingMetricsConsumer
import backtype.storm.spout.SchemeAsMultiScheme
import backtype.storm.topology.TopologyBuilder
import backtype.storm.{LocalCluster, Config, StormSubmitter}
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.commons.logging.LogFactory
import storm.kafka.{KafkaSpout, SpoutConfig, StringScheme, ZkHosts}

import scala.util.control.NonFatal

/**
 * A sample Storm topology implementation
 * @param topic The Kafka topic
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
class SampleTopology(topic : String) {

  private val log = LogFactory.getLog(getClass())

  val zkHosts = new ZkHosts(StormConfig.zookeeper)
  val zookeeper = StormConfig.zookeeper

  log.info("ZooKeeper: " + zookeeper)

  val zkRoot = "/kafka-spout"
  // The spout appends this id to zkRoot when composing its ZooKeeper path.  You don't need a leading `/`.
  val zkSpoutId = "kafka-storm-pchr"

  val kafkaConfig = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId)
  kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme())
  kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime
  kafkaConfig.forceFromStart = true
  val kafkaSpout = new KafkaSpout(kafkaConfig)

  val builder = new TopologyBuilder
  val spoutId = "kafka"

  val parallel = 10

  builder.setSpout(spoutId, kafkaSpout, parallel)

  builder.setBolt("pchrNormalizerBolt",  new PCHRNormalizerBolt,  2).shuffleGrouping(spoutId)
  builder.setBolt("pchrEventsBolt",      new PCHREventsBolt,      2).shuffleGrouping("pchrNormalizerBolt")
  builder.setBolt("pchrEnrichBolt",      new PCHREnrichBolt,      2).shuffleGrouping("pchrEventsBolt")
  builder.setBolt("pchrGeolocationBolt", new PCHRGeolocationBolt, 2).shuffleGrouping("pchrEnrichBolt")
  builder.setBolt("pchrPersistenceBolt", new PCHRPersistenceBolt, 8).shuffleGrouping("pchrGeolocationBolt")

  // Showcases how to customize the topology configuration
  val topologyConfiguration = {
    val c = new Config
    c.setDebug(false)
    c.setNumWorkers(1)
    c.setMaxSpoutPending(100)
    c.setMessageTimeoutSecs(60)
    c.setNumAckers(1)
//    c.setMaxTaskParallelism(50)
//    c.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384: Integer)
//    c.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384: Integer)
    c.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 4: Integer)
//    c.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32: Integer)
    c.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 0.05: java.lang.Double)
    c.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1: java.lang.Integer)
    c
  }
  topologyConfiguration.registerMetricsConsumer(classOf[LoggingMetricsConsumer], 1)

  val topologyName = "pchr-topology"

  /**
   * Starts the [[backtype.storm.spout.ISpout]] execution
   * @param local <code>true</code> to start topology locally, false otherwise
   */
  def start(local: Boolean): Unit = {
    try {
      if (local) {
        log.info("Submitting local Storm topology")

        val cluster = new LocalCluster()
        cluster.submitTopology(topologyName, topologyConfiguration, builder.createTopology())

        // Graceful shutdown hook
        Runtime.getRuntime.addShutdownHook(new Thread() {
          override def run() {
            log.info("Shutting down topology " + topologyName)
            cluster.shutdown()
          }
        })
      } else {
        log.info("Submitting remote Storm topology")
        StormSubmitter.submitTopology(topologyName, topologyConfiguration, builder.createTopology())
      }
    } catch {
      case NonFatal(e) => {
        log.error(ExceptionUtils.getStackTrace(e))
        System.exit(1)
      }
    }

  }
}



