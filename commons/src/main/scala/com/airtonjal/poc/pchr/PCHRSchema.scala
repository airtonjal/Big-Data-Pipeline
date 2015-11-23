package com.airtonjal.poc.pchr

/**
 * Fields schema on the pchr processing
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
object PCHRSchema {

  val CALL                   = "Call"
  val START_TIME_NAME        = "StartTime"
  val IMEI_TAC               = "TAC"
  val PHONE                  = "Phone"

  val EVENT_TYPE_NAME        = "EventType"
  val EVENTS_NAME            = "Events"
  val EVENT_NAME             = "EventName"
  val MEASUREMENTS_NAME      = "Measurements"
  val CAUSE_NAME             = "Cause"
  val FAIL_CAU_CHOICE_NAME   = "FailCauChoice"
  val ITF_FAIL_DESC_NAME     = "ItfFailDesc"

  val CELL_EVAL_CONNECTED = 90
  val CELL_EVAL_CONNECTED_NEIGHBOUR = 91
  val CELL_EVAL_ACTIVE = 0
  val CELL_EVAL_MONITORED = 1
  val CELL_EVAL_TARGET = 92

  // Measurement
  val CELL_ID                = "CellId"
  val RNC_ID                 = "RNCId"
  val CELL_EVAL_TYPE_ID_NAME = "CellEvalTypeId"
  val CELL_RSCP              = "CellRscp"
  val CELL_EC_N0             = "CellEc_N0"
  val PROPG_DELAY            = "PropgDelay"
  val FRAME_OFFSET           = "FrameOffset"
  val CHIP_OFFSET            = "ChipOffset"
  val SIG_DELAY              = "SigDelay"

  // Events
  val CALL_ESTABLISHED_NAME = "Call Established"
  val CALL_ESTABLISHED_TYPE = 204

  val NORMAL_CALL_END_NAME = "Normal Call End"
  val NORMAL_CALL_END_TYPE = 203

  val DROPPED_CALL_NAME = "Dropped Call"
  val DROPPED_CALL_TYPE = 11

  val BLOCKED_CALL_NAME = "Dropped Call"
  val BLOCKED_CALL_TYPE = 10

  val MEASUREMENT_REPORT_NAME = "Measurement Report"
  val MEASUREMENT_REPORT_TYPE = 226

  val SHO_ABN_NAME = "Soft Handover Abnormal"
  val SHO_NORMAL_NAME = "Soft Handover Normal"
  val SHO_ABN_TYPE = 14
  val SHO_NORMAL_TYPE = 245

  val E1C_NAME = "E1C"
  val E1C_TYPE = 1002

  val E1A_NAME = "E1A"
  val E1A_TYPE = 1001

  // GeoLocation
  val GEOLOCATION = "Geolocation"
  val METHOD = "Method"
  val LATITUDE  = "Lat"
  val LONGITUDE = "Lon"
}
