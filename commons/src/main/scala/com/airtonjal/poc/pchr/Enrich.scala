package com.airtonjal.poc.pchr

import com.fasterxml.jackson.databind.{ObjectMapper, JsonNode}
import com.fasterxml.jackson.databind.node._
import com.airtonjal.poc.cell.CellsInfo
import com.airtonjal.poc.json.JsonUtils._

import PCHRSchema._
import com.airtonjal.poc.phone.PhoneInfo
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * Enrichment object to inject cell information properties into events measurements
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
object Enrich {

  private val log = LoggerFactory.getLogger(getClass)
  val mapper = new ObjectMapper

  /**
   * Injects cells location properties
   * @param root The root pchr json
   */
  def inject(root: JsonNode): Unit = {
    findNode(root, Some(EVENTS_NAME)) match {
      case Some(events: ArrayNode) => {
        for(event <- events) {
          findNode(event, Some(MEASUREMENTS_NAME)) match {
            case Some(measurements: ArrayNode) => {
              for(measurement <- measurements) {
                findNode(measurement, Some(PCHRSchema.CELL_ID.toString)) match {
                  case Some(cellIdNode: IntNode) => {
                    val cellId = cellIdNode.asInt()
                    CellsInfo.site(cellId) match { case Some(site) => add(measurement, new TextNode(site), CellsInfo.Fields.SITE.toString) case _ => }
                    CellsInfo.lat(cellId) match { case Some(lat) => add(measurement, new DoubleNode(lat), CellsInfo.Fields.LAT.toString) case _ => }
                    CellsInfo.lon(cellId) match { case Some(lon) => add(measurement, new DoubleNode(lon), CellsInfo.Fields.LON.toString) case _ => }
                    CellsInfo.azimuth(cellId) match { case Some(azimuth) => add(measurement, new IntNode(azimuth), CellsInfo.Fields.AZIMUTH.toString) case _ => }
                    CellsInfo.antennaHeight(cellId) match { case Some(antennaHeight) => add(measurement, new IntNode(antennaHeight), CellsInfo.Fields.ANTENNA_HEIGHT.toString) case _ => }
                    CellsInfo.maxTxPower(cellId) match { case Some(maxTxPower) => add(measurement, new IntNode(maxTxPower), CellsInfo.Fields.MAX_TX_POWER.toString) case _ => }
                    CellsInfo.dlFcMhz(cellId) match { case Some(dlFcMhz) => add(measurement, new DoubleNode(dlFcMhz), CellsInfo.Fields.DL_FC_MHZ.toString) case _ => }
                  }
                  case _ => log.trace("Weird behaviour, no " + PCHRSchema.CELL_ID.toString + " found")
                }
              }
            }
            case _ => // No measurements found
          }
        }
      }
      case _ => // No events found
    }

    findNode(root, Some(CALL)) match {
      case Some(call: ObjectNode) => {
        findNode(call, Some(IMEI_TAC)) match {
          case Some(tac: TextNode) => {
            PhoneInfo.cellsMap.get(tac.asText) match {
              case Some(tacMap: Map[String, String]) => {
                add(call, mapper.createObjectNode(), PHONE)
                tacMap.filter(t => t._2 != "").foreach(t => add(call, new TextNode(t._2), PHONE + "." + t._1))
              }
              case None => log.trace("TAC " + tac.asText() + " not found in PhoneInfo map")
            }
          }
          case _ => log.trace("TAC not found in pchr")
        }
      }
      case _ => log.warn("No " + CALL + " node found in pchr, something weird might have happened")
    }
  }

}
