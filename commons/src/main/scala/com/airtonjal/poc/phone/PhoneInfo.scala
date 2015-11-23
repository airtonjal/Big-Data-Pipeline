package com.airtonjal.poc.phone

import com.airtonjal.poc.csv.CSVAccessor
import org.slf4j.LoggerFactory

/**
 * Phone model index
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
object PhoneInfo extends CSVAccessor {

  private val log = LoggerFactory.getLogger(getClass)

  /** Phone models CSV filename */
  def getFilename() = "tac.csv"
  def getSeparator() = ','

  object Fields extends Enumeration {
    type Fields = Value
    val TAC                       = Value("tac")
    val PHONE_MODEL               = Value("phoneModel")
    val DEVICE_TYPE               = Value("deviceType")
    val IPHONE_MODEL              = Value("iphoneModel")
  }

  /** Field to create maps  */
  private val INDEX_BY = Fields.TAC.toString

  // Converts each tac to an integer and then applyTransforms to each map
  val cellsMap = lines.groupBy(map => map.get(INDEX_BY).get).mapValues(l => l.head)

  private def getField(tac: String, fieldName: String) =
    cellsMap.get(tac) match {
      case Some(map) => map.get(fieldName)
      case None => None
    }

  def phoneModel (tac: String): Option[String] = getField(tac, Fields.PHONE_MODEL.toString)
  def deviceType (tac: String): Option[String] = getField(tac, Fields.DEVICE_TYPE.toString)
  def iphoneModel(tac: String): Option[String] = getField(tac, Fields.IPHONE_MODEL.toString)

}
