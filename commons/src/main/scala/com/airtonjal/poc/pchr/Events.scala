package com.airtonjal.poc.pchr

import com.fasterxml.jackson.databind.node.{ArrayNode, TextNode, IntNode}
import com.fasterxml.jackson.databind.{ObjectMapper, JsonNode}
import com.airtonjal.poc.json.JsonUtils._
import PCHRSchema._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * Creates events from inside PCHR
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
object Events {

  private val log = LoggerFactory.getLogger(getClass)

  val mapper = new ObjectMapper

  def createEvents(root: JsonNode): Unit = {
    val events = mapper.createArrayNode()
    add(root, events, EVENTS_NAME)

    def addEvent(eventCreator: JsonNode => Array[(JsonNode, String)]): Unit = {
      val eventNodes = eventCreator(root)

      for(eventAndName <- eventNodes) {
        val event = eventAndName._1
        val name  = eventAndName._2

        addToArray(events, event)
        add(event, new TextNode(name), EVENT_NAME)
      }
    }
                                        addEvent(callEstablishedEvent) // Call Established event, always occurs
    if (verifyNormalCallEnd(root))      addEvent(normalCallEndEvent)
    if (verifyDroppedCall(root))        addEvent(droppedCallEvent)
    if (verifyBlockedCall(root))        addEvent(blockedCallEvent)
    if (verifyMeasurementReport(root))  addEvent(measurementReportEvent)
    if (verifySoftHandover(root))       addEvent(softHandoverEvent)
    if (verifyE1C(root))                addEvent(e1CEvent)
    if (verifyE1A(root))                addEvent(e1AEvent)
  }

  private def callEstablishedEvent(root: JsonNode): Array[(JsonNode, String)] = {
    val eventNode = mapper.createObjectNode()
    val measurements = mapper.createArrayNode()
    add(eventNode, measurements, MEASUREMENTS_NAME)

    copy(root, eventNode, Some("CommonInfo.CommGenInfo.ConnSetupTime"), START_TIME_NAME)
    add(eventNode, new IntNode(CALL_ESTABLISHED_TYPE), EVENT_TYPE_NAME)

    if (exists(root, Some("SingAccess.SigAccTit"))) {
      val measurement = mapper.createObjectNode()
      copy(root, measurement, Some("SingAccess.SigAccTit.SigSetCellId"), CELL_ID)
      copy(root, measurement, Some("SingAccess.SigAccTit.SigSetRNCId"), RNC_ID)
      add(measurement, new IntNode(CELL_EVAL_CONNECTED), CELL_EVAL_TYPE_ID_NAME)
      copy(root, measurement, Some("SingAccess.SigRFInfo.SigSetRscp"), CELL_RSCP)
      copy(root, measurement, Some("SingAccess.SigRFInfo.SigSetEc_N0"), CELL_EC_N0)
      copy(root, measurement, Some("CommonInfo.CommGenInfo.PropgDelay"), PROPG_DELAY)
      copy(root, measurement, Some("SingAccess.SigAccTit.FrameOffset"), FRAME_OFFSET)
      copy(root, measurement, Some("SingAccess.SigAccTit.ChipOffset"), CHIP_OFFSET)
      copy(root, measurement, Some("SingAccess.SigAccTit.SigSetDelay"), SIG_DELAY)

      addToArray(measurements, measurement)
    }

    findNode(root, Some("SingAccess.SigRFInfo.ConnNCellCnts")) match {
      case Some(array: ArrayNode) => {
        for(node <- array) {
          val measurement = mapper.createObjectNode()
          copy(node, measurement, Some("ConnNCellId"), CELL_ID)
          copy(node, measurement, Some("ConnNRNCId"), RNC_ID)
          add(measurement, new IntNode(CELL_EVAL_CONNECTED_NEIGHBOUR), CELL_EVAL_TYPE_ID_NAME)
          copy(node, measurement, Some("ConnNCellRscp"), CELL_RSCP)
          copy(node, measurement, Some("ConnNCellEc_N0"), CELL_EC_N0)

          addToArray(measurements, measurement)
        }
      }
      case _ =>
    }

    Array((eventNode, CALL_ESTABLISHED_NAME))
  }

  private def verifyNormalCallEnd(root: JsonNode): Boolean =
    exists(root, Some("RrcRelInfo.RrcRelTitInfo")) && !exists(root, Some("RrcRelInfo.RrcRelAbnInfo"))
  private def normalCallEndEvent(root: JsonNode): Array[(JsonNode, String)] = {
    val eventNode = mapper.createObjectNode()
    val measurements = mapper.createArrayNode()
    add(eventNode, measurements, MEASUREMENTS_NAME)

    copy(root, eventNode, Some("RrcRelInfo.RrcRelTitInfo.RrcRelTime"), START_TIME_NAME)
    add(eventNode, new IntNode(NORMAL_CALL_END_TYPE), EVENT_TYPE_NAME)
    copy(root, eventNode, Some("RrcRelInfo.RrcRelTitInfo.RrcIuCausType"), CAUSE_NAME)

    findNode(root, Some("RrcRelInfo.RrcRelTitInfo.RrcActCelCnts")) match {
      case Some(array: ArrayNode) => {
        for(node <- array) {
          val measurement = mapper.createObjectNode()
          copy(node, measurement, Some( "RrcActCellId"), CELL_ID)
          copy(node, measurement, Some("RrcActRNCId"), RNC_ID)
          add(measurement, new IntNode(CELL_EVAL_ACTIVE), CELL_EVAL_TYPE_ID_NAME)

          addToArray(measurements, measurement)
        }
      }
      case _ =>
    }

    Array((eventNode, NORMAL_CALL_END_NAME))
  }

  private def verifyDroppedCall(root: JsonNode): Boolean = {
    val bit2RabLossInd = findNode(root, Some("RABInfo.RABTitInfo.bit2RabLossInd"))
    return exists(root, Some("RrcRelInfo.RrcRelAbnInfo")) && bit2RabLossInd.isDefined && bit2RabLossInd.get.asInt() == 1
  }
  private def droppedCallEvent(root: JsonNode): Array[(JsonNode, String)] = {
    val eventNode = mapper.createObjectNode()
    val measurements = mapper.createArrayNode()
    add(eventNode, measurements, MEASUREMENTS_NAME)

    copy(root, eventNode, Some("RrcRelInfo.RrcRelAbnInfo.RrcRelTime"), START_TIME_NAME)
    add(eventNode, new IntNode(DROPPED_CALL_TYPE), EVENT_TYPE_NAME)
    copy(root, eventNode, Some("RrcRelInfo.RrcRelAbnInfo.RrcErrorCause"), CAUSE_NAME)
    copy(root, eventNode, Some("RrcRelInfo.RrcRelAbnInfo.RrcFailCauChoice"), FAIL_CAU_CHOICE_NAME)
    copy(root, eventNode, Some("RrcRelInfo.RrcRelAbnInfo.RrcItfFailDesc"), ITF_FAIL_DESC_NAME)

    findNode(root, Some("RrcRelInfo.RrcRelTitInfo.RrcActCelCnts")) match {
      case Some(array: ArrayNode) => {
        for(node <- array) {
          val measurement = mapper.createObjectNode()
          copy(node, measurement, Some("RrcActCellId"), CELL_ID)
          copy(node, measurement, Some("RrcActRNCId"), RNC_ID)
          add(measurement, new IntNode(CELL_EVAL_ACTIVE), CELL_EVAL_TYPE_ID_NAME)

          addToArray(measurements, measurement)
        }
      }
      case _ =>
    }

    Array((eventNode, DROPPED_CALL_NAME))
  }

  private def verifyBlockedCall(root: JsonNode): Boolean = {
    val bit2RabLossInd = findNode(root, Some("RABInfo.RABTitInfo.bit2RabLossInd"))
    if (bit2RabLossInd.isEmpty) return false
    val bit2RabLossIndInt = bit2RabLossInd.get.asInt()
    exists(root, Some("RrcRelInfo.RrcRelAbnInfo")) &&
      (bit2RabLossIndInt == 0 | bit2RabLossIndInt == 1 || bit2RabLossIndInt == 2)
  }
  private def blockedCallEvent(root: JsonNode): Array[(JsonNode, String)] = {
    val eventNode = mapper.createObjectNode()
    val measurements = mapper.createArrayNode()
    add(eventNode, measurements, MEASUREMENTS_NAME)

    copy(root, eventNode, Some("RrcRelInfo.RrcRelTitInfo.RrcRelTime"), START_TIME_NAME)
    add(eventNode, new IntNode(BLOCKED_CALL_TYPE), EVENT_TYPE_NAME)
    copy(root, eventNode, Some("RrcRelInfo.RrcRelAbnInfo.RrcErrorCause"), CAUSE_NAME)
    copy(root, eventNode, Some("RrcRelInfo.RrcRelAbnInfo.RrcFailCauChoice"), FAIL_CAU_CHOICE_NAME)
    copy(root, eventNode, Some("RrcRelInfo.RrcRelAbnInfo.RrcItfFailDesc"), ITF_FAIL_DESC_NAME)

    findNode(root, Some("RrcRelInfo.RrcRelTitInfo.RrcActCelCnts")) match {
      case Some(array: ArrayNode) => {
        for(node <- array) {
          val measurement = mapper.createObjectNode()
          copy(node, measurement, Some("RrcActCellId"), CELL_ID)
          copy(node, measurement, Some("RrcActRNCId"), RNC_ID)
          add(measurement, new IntNode(CELL_EVAL_ACTIVE), CELL_EVAL_TYPE_ID_NAME)

          addToArray(measurements, measurement)
        }
      }
      case _ =>
    }

    Array((eventNode, BLOCKED_CALL_NAME))
  }

  private def verifyMeasurementReport(root: JsonNode): Boolean =
    exists(root, Some("RrcRelInfo.RrcRFInfo.RrcRelRptCnts"))
  private def measurementReportEvent(root: JsonNode): Array[(JsonNode, String)] = {
    findNode(root, Some("RrcRelInfo.RrcRFInfo.RrcRelRptCnts")) match {
      case Some(array: ArrayNode) => {
        var events = Array[(JsonNode, String)]()

        for(node <- array) {
          val eventNode = mapper.createObjectNode()

          copy(node, eventNode, Some("RrcRptTime"), START_TIME_NAME)
          add(eventNode, new IntNode(MEASUREMENT_REPORT_TYPE), EVENT_TYPE_NAME)

          findNode(node, Some("RrcRelRFCellNums")) match {
            case Some(measArray: ArrayNode) => {
              val measurements = mapper.createArrayNode()
              add(eventNode, measurements, MEASUREMENTS_NAME)

              for(meas <- measArray) {
                val measurement = mapper.createObjectNode()
                copy(meas, measurement, Some("RrcRFCellId"), CELL_ID)
                copy(meas, measurement, Some("RrcRFRNCId"), RNC_ID)

                findNode(meas, Some("bitCellSet")) match {
                  case Some(bitCellSet) => {
                    val bitCellSetStr = bitCellSet.asText()

                    val cellEvalTypeId = bitCellSetStr.substring(bitCellSetStr.indexOf('(') + 1, bitCellSetStr.indexOf(')')).toInt
                    add(measurement, new IntNode(cellEvalTypeId), CELL_EVAL_TYPE_ID_NAME)

                  }
                  case None =>
                }

                copy(meas, measurement, Some("RrcRFRscp"), CELL_RSCP)
                copy(meas, measurement, Some("RrcRFEc_N0"), CELL_EC_N0)

                addToArray(measurements, measurement)
              }
              
            }
            case _ =>
          }

          events = events :+ (eventNode, MEASUREMENT_REPORT_NAME)
        }

        events
      }
      case _ => Array.empty
    }
  }

  private def verifySoftHandover(root: JsonNode): Boolean = exists(root, Some("ShoInfos"))
  private def softHandoverEvent(root: JsonNode): Array[(JsonNode, String)] = {
    findNode(root, Some("ShoInfos")) match {
      case Some(shoArray: ArrayNode) => {
        var events = Array[(JsonNode, String)]()

        for(node <- shoArray) {
          val eventNode = mapper.createObjectNode()

          val abnormal = exists(node, Some("ShoAbnInfo"))

          var eventName: String = null
          copy(node, eventNode, Some("ShoTitInfo.ShoBegTime"), START_TIME_NAME)
          if (abnormal) {
            eventName = SHO_ABN_NAME
            add(eventNode, new IntNode(SHO_ABN_TYPE), EVENT_TYPE_NAME)
            copy(node, eventNode, Some("ShoAbnInfo.ShoFailCauChoice"), FAIL_CAU_CHOICE_NAME)
            copy(node, eventNode, Some("ShoAbnInfo.ShoItFailDesc"), ITF_FAIL_DESC_NAME)
            copy(node, eventNode, Some("ShoAbnInfo.ShoErrorCause"), CAUSE_NAME)
          } else {
            eventName = SHO_NORMAL_NAME
            add(eventNode, new IntNode(SHO_NORMAL_TYPE), EVENT_TYPE_NAME)
          }

          val measurements = mapper.createArrayNode()

          findNode(node, Some("ShoRFInfo.ShoActCelNums")) match {
            case Some(measArray: ArrayNode) => {
              for(meas <- measArray) {
                val measurement = mapper.createObjectNode()
                copy(meas, measurement, Some("ShoActCellId"), CELL_ID)
                copy(meas, measurement, Some("ShoActRNCId"), RNC_ID)
                add(meas, new IntNode(CELL_EVAL_ACTIVE), CELL_EVAL_TYPE_ID_NAME)
                copy(meas, measurement, Some("ShoActCelRcsp"), CELL_RSCP)
                copy(meas, measurement, Some("ShoActCelEc_N0"), CELL_EC_N0)

                addToArray(measurements, measurement)
              }
            }
            case _ =>
          }

          findNode(node, Some("ShoRFInfo.ShoMonCelNums")) match {
            case Some(measArray: ArrayNode) => {
              for(meas <- measArray) {
                val measurement = mapper.createObjectNode()
                copy(meas, measurement, Some("ShoMonCellId"), CELL_ID)
                copy(meas, measurement, Some("ShoMonRNCId"), RNC_ID)
                add(meas, new IntNode(CELL_EVAL_MONITORED), CELL_EVAL_TYPE_ID_NAME)
                copy(meas, measurement, Some("ShoMonCelRscp"), CELL_RSCP)
                copy(meas, measurement, Some("ShoMonCelEc_N0"), CELL_EC_N0)

                addToArray(measurements, measurement)
              }
            }
            case _ =>
          }

          findNode(node, Some("ShoRFInfo.ShoTarCelNums")) match {
            case Some(measArray: ArrayNode) => {
              for(meas <- measArray) {
                val measurement = mapper.createObjectNode()
                copy(meas, measurement, Some("ShoTarCellId"), CELL_ID)
                copy(meas, measurement, Some("ShoTarRNCId"), RNC_ID)
                add(meas, new IntNode(CELL_EVAL_TARGET), CELL_EVAL_TYPE_ID_NAME)
                copy(meas, measurement, Some("ShoFirstNewFraOfs"), FRAME_OFFSET)
                copy(meas, measurement, Some("ShoFirstNewChpOfs"), CHIP_OFFSET)

                addToArray(measurements, measurement)
              }

            }
            case _ =>
          }

          if (measurements.size() > 0) add(eventNode, measurements, MEASUREMENTS_NAME)

          events = events :+ (eventNode, eventName)
        }

        events
      }
      case _ => Array.empty
    }
  }

  private def verifyE1C(root: JsonNode): Boolean = exists(root, Some("NetOptInfo.FrePolMon.Eve1CUcCnts"))
  private def e1CEvent(root: JsonNode): Array[(JsonNode, String)] = {
    findNode(root, Some("NetOptInfo.FrePolMon.Eve1CUcCnts")) match {
      case Some(netOptArray: ArrayNode) => {
        var events = Array[(JsonNode, String)]()

        for(node <- netOptArray) {
          val eventNode = mapper.createObjectNode()

          copy(node, eventNode, Some("Eve1CRepTime"), START_TIME_NAME)
          add(eventNode, new IntNode(E1C_TYPE), EVENT_TYPE_NAME)

          val measurements = mapper.createArrayNode()

          findNode(node, Some("ActCelCnt_1Cs")) match {
            case Some(measArray: ArrayNode) => {
              for(meas <- measArray) {
                val measurement = mapper.createObjectNode()
                copy(meas, measurement, Some("Act1CCellId"), CELL_ID)
                copy(meas, measurement, Some("Act1CRNCId"), RNC_ID)
                add(meas, new IntNode(CELL_EVAL_ACTIVE), CELL_EVAL_TYPE_ID_NAME)
                copy(meas, measurement, Some("ActCel1CRscp"), CELL_RSCP)
                copy(meas, measurement, Some("ActCel1CEc_N0"), CELL_EC_N0)

                addToArray(measurements, measurement)
              }
            }
            case _ =>
          }

          findNode(node, Some("MonCelCnt_1Cs")) match {
            case Some(measArray: ArrayNode) => {
              for(meas <- measArray) {
                val measurement = mapper.createObjectNode()
                copy(meas, measurement, Some("Mon1CCellId"), CELL_ID)
                copy(meas, measurement, Some("Mon1CRNCId"), RNC_ID)
                add(meas, new IntNode(CELL_EVAL_MONITORED), CELL_EVAL_TYPE_ID_NAME)
                copy(meas, measurement, Some("MonCel1CRscp"), CELL_RSCP)
                copy(meas, measurement, Some("MonCel1CEc_N0"), CELL_EC_N0)

                addToArray(measurements, measurement)
              }
            }
            case _ =>
          }

          if (measurements.size() > 0) add(eventNode, measurements, MEASUREMENTS_NAME)

          events = events :+ (eventNode, E1C_NAME)
        }

        events
      }
      case _ => Array.empty
    }
  }

  private def verifyE1A(root: JsonNode): Boolean = exists(root, Some("NetOptInfo.IntraNCelOpt.Eve1AUcCnts"))
  private def e1AEvent(root: JsonNode): Array[(JsonNode, String)] = {
    findNode(root, Some("NetOptInfo.IntraNCelOpt.Eve1AUcCnts")) match {
      case Some(netOptArray: ArrayNode) => {
        var events = Array[(JsonNode, String)]()

        for(node <- netOptArray) {
          val eventNode = mapper.createObjectNode()

          copy(node, eventNode, Some("Eve1ARepTime"), START_TIME_NAME)
          add(eventNode, new IntNode(E1A_TYPE), EVENT_TYPE_NAME)

          val measurements = mapper.createArrayNode()

          findNode(node, Some("ActCelCnt_1As")) match {
            case Some(measArray: ArrayNode) => {
              for(meas <- measArray) {
                val measurement = mapper.createObjectNode()
                copy(meas, measurement, Some("Act1ACellId"), CELL_ID)
                copy(meas, measurement, Some("Act1ARNCId"), RNC_ID)
                add(meas, new IntNode(CELL_EVAL_ACTIVE), CELL_EVAL_TYPE_ID_NAME)
                copy(meas, measurement, Some("ActCel1ARscp"), CELL_RSCP)
                copy(meas, measurement, Some("ActCel1AEc_N0"), CELL_EC_N0)

                addToArray(measurements, measurement)
              }
            }
            case _ =>
          }

          findNode(node, Some("MonCelCnt_1As")) match {
            case Some(measArray: ArrayNode) => {
              for(meas <- measArray) {
                val measurement = mapper.createObjectNode()
                copy(meas, measurement, Some("Mon1ACellId"), CELL_ID)
                copy(meas, measurement, Some("Mon1ARNCId"), RNC_ID)
                add(meas, new IntNode(CELL_EVAL_MONITORED), CELL_EVAL_TYPE_ID_NAME)
                copy(meas, measurement, Some("MonCel1ARscp"), CELL_RSCP)
                copy(meas, measurement, Some("MonCel1AEc_N0"), CELL_EC_N0)

                addToArray(measurements, measurement)
              }
            }
            case _ =>
          }

          if (measurements.size() > 0) add(eventNode, measurements, MEASUREMENTS_NAME)

          events = events :+ (eventNode, E1A_NAME)
        }

        events
      }
      case _ => Array.empty
    }
  }

}
