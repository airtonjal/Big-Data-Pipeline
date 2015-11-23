package com.airtonjal.poc.cell

import com.airtonjal.poc.csv
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * Object to hold cells information (location, site, name, etc)
 * Source files comes as a semicolon separated values
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
object CellsInfo extends csv.CSVAccessor {
  private val log = LoggerFactory.getLogger(getClass)

  /** Cells topology CSV filename */
  def getFilename() = "cell_info.csv"
  def getSeparator() = ';'

  object Fields extends Enumeration {
    type Fields = Value
    val NETWORK                       = Value("network")
    val NETWORK_ID                    = Value("networkId")
    val REGION                        = Value("region")
    val SITE                          = Value("site")
    val CELL                          = Value("cell")
    val LAT                           = Value("lat")
    val LON                           = Value("lon")
    val AZIMUTH                       = Value("azimuth")
    val ANTENNA_HEIGHT                = Value("antennaHeight")
    val RNC_ID                        = Value("rncId")
    val CELL_ID                       = Value("cellId")
    val ACT_STATUS                    = Value("actStatus")
    val DL_POWER_AVERAGE_WINDOW_SIZE  = Value("dlPowerAverageWindowSize")
    val LAC                           = Value("lac")
    val LO_CELL                       = Value("loCell")
    val MAX_TX_POWER                  = Value("maxTXPower")
    val MBMSACT_FLAG                  = Value("mbmsactFlg")
    val MIMOACT_FLAG                  = Value("mimoactFlag")
    val NODEB_NAME                    = Value("nodeBName")
    val PSC_CRAMB_CODE                = Value("pscCrambCode")
    val SAC                           = Value("sac")
    val UARFCN_DOWNLINK               = Value("uarfcnDownlink")
    val UARFCN_UPLINK                 = Value("uarfcnUplink")
    val UARFCN_UPLINK_IND             = Value("uarfcnUplinkInd")
    val MINPCPICH_POWER               = Value("minpcpichPower")
    val PCPICH_POWER                  = Value("pcpichPower")
    val THE_GEOM                      = Value("theGeom")
    val SECTOR                        = Value("sector")
    val SECTOR_CENTROID               = Value("sectorCentroid")
    val MCC                           = Value("mcc")
    val MNC                           = Value("mnc")
    val CGI                           = Value("cgi")
    val UL_FC_MHZ                     = Value("ulFcMhz")
    val DL_FC_MHZ                     = Value("dlFcMhz")
    val BAND_INDICATOR                = Value("bandIndicator")
  }

  /** Field to create maps  */
  private val INDEX_BY = Fields.CELL_ID.toString

  /** Map to indicate which fields in the csv should be treated as doubles instead of Strings */
  private val doubleFields = Set(Fields.LON.toString, Fields.LAT.toString, Fields.UL_FC_MHZ.toString, Fields.DL_FC_MHZ.toString)

  /** Map to indicate which fields in the csv should be treated as integers instead of Strings */
  private val intFields = Set(Fields.MAX_TX_POWER.toString, Fields.NETWORK_ID.toString, Fields.BAND_INDICATOR.toString,
    Fields.UARFCN_UPLINK.toString, Fields.RNC_ID.toString, Fields.LO_CELL.toString, Fields.PSC_CRAMB_CODE.toString,
    Fields.SAC.toString, Fields.UARFCN_DOWNLINK, Fields.MNC.toString, Fields.ANTENNA_HEIGHT.toString, Fields.CELL_ID,
    Fields.MCC.toString, Fields.LAC.toString, Fields.AZIMUTH.toString)

  /** Map to indicate which fields in the csv should be treated as booleans instead of Strings */
  private val booleanFields = Set(Fields.UARFCN_UPLINK_IND.toString)

  // Converts each cellId to an integer and then applyTransforms to each map
  val cellsMap = lines.groupBy(map => map.get(INDEX_BY).get.toInt).mapValues(list => applyTransforms(list(0)))

  /**
   * Transforms a [[scala.collection.immutable.Map]] [[String]] [[String]] into a [[scala.collection.mutable.Map]]
   * [[String]] [[Any]] by converting some fields indicated by the
   * intFields, doubleFields and booleanFields sets
   * @param map The cell key-value [[Map]]
   * @return
   */
  private def applyTransforms(map: Map[String, String]): mutable.Map[String, Any] = {
    val transformedMap: mutable.Map[String, Any] = new mutable.HashMap()
    map.filter(_._2 != "")foreach(t =>
      try {
        if (doubleFields.contains(t._1)) transformedMap += t._1 -> t._2.toDouble
        else if (intFields.contains(t._1)) transformedMap += t._1 -> t._2.toInt
        else if (booleanFields.contains(t._1)) {
          if (t._2 == "TRUE") transformedMap += t._1 -> true
          if (t._2 == "FALSE") transformedMap += t._1 -> false
        }
        else transformedMap += t._1 -> t._2
      } catch {
        case nfe: NumberFormatException => log.error("Could not convert number field \'" + t._1 + "\'\twith value: " + t._2 + "\n", nfe)
      }
    )

    transformedMap
  }

  private def typed[V](cellId: Int, field: String): Option[V] = cellsMap.get(cellId) match {
    case Some(map) => map.get(field) match {
      case Some(value) => Some(value.asInstanceOf[V])
      case None => None
    }
    case None => None
  }

  private def asString (cellId: Int, field: String) = typed[String] (cellId, field)
  private def asInt    (cellId: Int, field: String) = typed[Int]    (cellId, field)
  private def asDouble (cellId: Int, field: String) = typed[Double] (cellId, field)
  private def asBoolean(cellId: Int, field: String) = typed[Boolean](cellId, field)
//
  def network                  (cellId: Int) = asString(cellId, Fields.NETWORK.toString)
  def networkId                (cellId: Int) = asInt   (cellId, Fields.NETWORK_ID.toString)
  def region                   (cellId: Int) = asString(cellId, Fields.REGION.toString)
  def site                     (cellId: Int) = asString(cellId, Fields.SITE.toString)
  def cell                     (cellId: Int) = asString(cellId, Fields.CELL.toString)
  def lat                      (cellId: Int) = asDouble(cellId, Fields.LAT.toString)
  def lon                      (cellId: Int) = asDouble(cellId, Fields.LON.toString)
  def azimuth                  (cellId: Int) = asInt   (cellId, Fields.AZIMUTH.toString)
  def antennaHeight            (cellId: Int) = asInt   (cellId, Fields.ANTENNA_HEIGHT.toString)
  def rncId                    (cellId: Int) = asInt   (cellId, Fields.RNC_ID.toString)
  def actStatus                (cellId: Int) = asString(cellId, Fields.ACT_STATUS.toString)
  def dlPowerAverageWindowSize (cellId: Int) = asString(cellId, Fields.DL_POWER_AVERAGE_WINDOW_SIZE.toString)
  def lac                      (cellId: Int) = asInt   (cellId, Fields.LAC.toString)
  def loCell                   (cellId: Int) = asInt   (cellId, Fields.LO_CELL.toString)
  def maxTxPower               (cellId: Int) = asInt   (cellId, Fields.MAX_TX_POWER.toString)
  def mbmsactFlag              (cellId: Int) = asString(cellId, Fields.MBMSACT_FLAG.toString)
  def mimoactFlag              (cellId: Int) = asString(cellId, Fields.MIMOACT_FLAG.toString)
  def nodeBName                (cellId: Int) = asString(cellId, Fields.NODEB_NAME.toString)
  def pscCrambCode             (cellId: Int) = asInt   (cellId, Fields.PSC_CRAMB_CODE.toString)
  def sac                      (cellId: Int) = asInt   (cellId, Fields.SAC.toString)
  def uarfcnDownlink           (cellId: Int) = asInt   (cellId, Fields.UARFCN_DOWNLINK.toString)
  def uarfcnUplink             (cellId: Int) = asInt   (cellId, Fields.UARFCN_UPLINK.toString)
  def uarfcnUplinkInd          (cellId: Int) = asString(cellId, Fields.UARFCN_UPLINK_IND.toString)
  def minpcpichPower           (cellId: Int) = asString(cellId, Fields.MINPCPICH_POWER.toString)
  def pcpichPower              (cellId: Int) = asString(cellId, Fields.PCPICH_POWER.toString)
  def theGeom                  (cellId: Int) = asString(cellId, Fields.THE_GEOM.toString)
  def sector                   (cellId: Int) = asString(cellId, Fields.SECTOR.toString)
  def sectorCentroid           (cellId: Int) = asString(cellId, Fields.SECTOR_CENTROID.toString)
  def mcc                      (cellId: Int) = asInt   (cellId, Fields.MCC.toString)
  def mnc                      (cellId: Int) = asInt   (cellId, Fields.MNC.toString)
  def cgi                      (cellId: Int) = asString(cellId, Fields.CGI.toString)
  def ulFcMhz                  (cellId: Int) = asDouble(cellId, Fields.UL_FC_MHZ.toString)
  def dlFcMhz                  (cellId: Int) = asDouble(cellId, Fields.DL_FC_MHZ.toString)
  def bandIndicator            (cellId: Int) = asString(cellId, Fields.BAND_INDICATOR.toString)

}
