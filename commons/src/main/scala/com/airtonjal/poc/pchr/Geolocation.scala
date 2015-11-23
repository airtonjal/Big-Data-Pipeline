package com.airtonjal.poc.pchr

import com.fasterxml.jackson.databind.{ObjectMapper, JsonNode}
import com.fasterxml.jackson.databind.node._
import com.airtonjal.poc.cell.CellsInfo
import com.airtonjal.poc.geolocation.trilateration.{NonLinearLeastSquaresSolver, TrilaterationFunction}
import com.airtonjal.poc.json.JsonUtils._
import com.airtonjal.poc.pchr.PCHRSchema._
import org.slf4j.LoggerFactory
import org.apache.commons.math3.fitting.leastsquares.LevenbergMarquardtOptimizer
import scala.collection.mutable.ListBuffer
import scala.math._

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

/**
 * GeoLocation simple implementation
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
object Geolocation {

  private val log = LoggerFactory.getLogger(getClass())

  val MOBILE_ANTENNA_HEIGHT = 1.5
  val CORRECTION_FACTOR = 3 // Between 0 and 3
  val MINIMUM_MEASUREMENTS = 3 // Minimum number of measurements to use trilateration algorithm
  val DL_FC_MHZ = 2152.6
  val mapper = new ObjectMapper

  def geolocate(root: JsonNode): Unit = {
    findNode(root, Some(EVENTS_NAME)) match {
      case Some(events: ArrayNode) => {
        for(event <- events) {
          findNode(event, Some(MEASUREMENTS_NAME)) match {
            case Some(measurements: ArrayNode) => {
              var positions = new ListBuffer[(Double, Double)]()
              var distances = new ListBuffer[Double]()

              var propgDelayNode = None : Option[JsonNode]

              var latitude  = None: Option[Double]
              var longitude = None: Option[Double]

              for (measurement <- measurements) {
                findNode(measurement, Some(PCHRSchema.PROPG_DELAY.toString)) match { // Tries to find a node with propagation delay
                  case Some(propNode: IntNode) => propgDelayNode = Some(measurement)
                  case _ =>
                }

                val latNode = findNode(measurement, Some(CellsInfo.Fields.LAT.toString))
                val lonNode = findNode(measurement, Some(CellsInfo.Fields.LON.toString))
  //                  val dlFcMhz = findNode(measurement, CellsInfo.Fields.DL_FC_MHZ.toString)
                val antennaHeightNode = findNode(measurement, Some(CellsInfo.Fields.ANTENNA_HEIGHT.toString))
                val rscpNode = findNode(measurement, Some(PCHRSchema.CELL_RSCP.toString))
                val maxTxPowerNode = findNode(measurement, Some(CellsInfo.Fields.MAX_TX_POWER.toString))

                (rscpNode, latNode, lonNode, antennaHeightNode, maxTxPowerNode) match {
                  case(Some(rscp: IntNode), Some(lat: DoubleNode), Some(lon: DoubleNode),
                  Some(antennaHeight: IntNode), Some(maxTxPower: IntNode)) => {
                    if (antennaHeight.asInt > 0 && rscp.asInt() != 0) {
                      val distance = calculateDistance(lat.asDouble(), lon.asDouble(),    DL_FC_MHZ, // Hardcoding because of csv value
                        antennaHeight.asDouble(), rscp.asDouble(), maxTxPower.asDouble())

                      positions = positions :+ (lon.asDouble(), lat.asDouble())
                      distances = distances :+ distance
                    }
                  }
                  case _ => log.trace("Not enough info to geolocate")
                }
              }

              var method: String = null
              propgDelayNode match {
                case Some(propMeasurement: ObjectNode) => {
                  method = "Azimuth"
                  val propgDelayNode = findNode(propMeasurement, Some(PCHRSchema.PROPG_DELAY.toString))
                  val latNode        = findNode(propMeasurement, Some(CellsInfo.Fields.LAT.toString))
                  val lonNode        = findNode(propMeasurement, Some(CellsInfo.Fields.LON.toString))
                  val azimuthNode    = findNode(propMeasurement, Some(CellsInfo.Fields.AZIMUTH.toString))
                  (propgDelayNode, latNode, lonNode, azimuthNode) match {
                    case(Some(propgDelay: IntNode), Some(lat: DoubleNode), Some(lon: DoubleNode), Some(azimuth: IntNode)) => {
                      doAzimuth(propgDelay.asInt, lon.asDouble, lat.asDouble, azimuth.asInt) match {
                        case Some(latLon) => longitude = Some(latLon._1); latitude  = Some(latLon._2)
                        case _ =>
                      }
                    }
                    case _ =>
                  }
                }
                case _ =>
                  try {
                    if (positions.size >= MINIMUM_MEASUREMENTS) {
                      method = "Trilateration"
                      val positionsArray = positions.map(t => Array(t._1, t._2)).toArray
                      val trilaterationFunction = new TrilaterationFunction(positionsArray, distances.toArray)
                      val solver = new NonLinearLeastSquaresSolver(trilaterationFunction, new LevenbergMarquardtOptimizer())
                      val optimum = solver.solve()

                      val latLon = optimum.getPoint.toArray

                      latitude  = Some(latLon(0))
                      longitude = Some(latLon(1))
                    }
                  } catch {
                    case NonFatal(error) => {
                      log.error(error.getMessage)
                      add(event, mapper.createObjectNode(), GEOLOCATION)
                      add(event, new TextNode(error.getMessage), GEOLOCATION + ".Exception")
                    }
                  }
              }

              (latitude, longitude) match {
                case (Some(lat: Double), Some(lon: Double)) => {
                  if (!event.has(LATITUDE)) {
                    add(event, mapper.createObjectNode(), GEOLOCATION)
                    add(event, new DoubleNode(lat), GEOLOCATION + "." + LATITUDE)
                    add(event, new DoubleNode(lon), GEOLOCATION + "." + LONGITUDE)
                    add(event, new TextNode(method), GEOLOCATION + "." + METHOD)
                  }
                }
                case _ =>
              }


            }
            case _ => // No measurements found
          }
        }
      }
      case _ => // No events found
    }

    remove(root, "CommonInfo")
    remove(root, "HhoInfo")
    remove(root, "RrcRelInfo")
    remove(root, "RrcRelTitInfo")
    remove(root, "RABInfo")
    remove(root, "StatInfo")
    remove(root, "SingAccess")
    remove(root, "ShoInfos")
    remove(root, "SysHoOut")
    remove(root, "NetOptInfo")
  }

  private def doAzimuth(propagationDelay: Int, longitude: Double, latitude: Double, azimuth: Int) : Option[(Double, Double)] = {
    distanceByPropagationDelay(propagationDelay) match {
      case Some(distInDegrees: Double) =>
        Some(longitude + distInDegrees * cos(reduceToRadians(azimuth)), latitude  + distInDegrees * sin(reduceToRadians(azimuth)))
      case _ =>
        None
    }
  }

  private def reduceToRadians(azimuth: Double) : Double =
    if (azimuth <= 90) (90  - azimuth) * Pi / 180
    else               (450 - azimuth) * Pi / 180

  private def distanceByPropagationDelay(propagationDelay : Int) : Option[Double] = {
    if (propagationDelay == 0) return None

    val distanceInMeters = propagationDelay * 234 + 117.0

    // Distancia equivalente em graus de latitude e longitude
    Some(distanceInMeters / 111000)
  }

  private def calculateDistance(lat: Double, lon: Double, dlFcMhz: Double, antennaHeight: Double, rscp: Double, maxTxPower: Double): Double = {
    // Fator de correção da altura da antena do móvel
    val a = 3.2 * pow(log10(11.75 * MOBILE_ANTENNA_HEIGHT), 2) - 4.97

    val loss = (maxTxPower/10) - rscp

    val distanceInKm = pow(10, (loss - 46.3 -33.9 * log10(dlFcMhz) + 13.82 * log10(antennaHeight) + a - CORRECTION_FACTOR) / (44.9 - 6.55 * log10(antennaHeight)))

    // Distancia equivamente em graus de latitude e longitude
    val distanceInDegrees = distanceInKm / 111

    return distanceInDegrees
  }

}
