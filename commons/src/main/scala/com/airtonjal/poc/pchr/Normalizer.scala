package com.airtonjal.poc.pchr

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{BooleanNode, TextNode, LongNode}
import com.airtonjal.poc.json.JsonUtils._
import com.airtonjal.poc.pchr.PCHRSchema._

/**
 * PCHR classifier sample implementation
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
object Normalizer {

  private val IMEI_TAC_NODE = "CommonInfo.CommGenInfo.IMEI_TAC"
  private val IMEI_SNR_NODE = "CommonInfo.CommGenInfo.IMEI_SNR"
  private val IMEI_SP_NODE  = "CommonInfo.CommGenInfo.IMEI_SP"

  private val SPEECH = "Speech"
  private val VIDEO = "Video"
  private val PS = "PS"
  private val EMERGENCY = "EMERGENCY"
  private val REGISTRATION = "REGISTRATION"
  private val SIGNALING = "SIGNALING"
  private val SIG_CAUSE_MAP = Map((0, SPEECH), (1, VIDEO), (2, PS), (3, PS), (4, PS), (5, SPEECH), (6, VIDEO), (7, PS), (8, PS),
    (9, EMERGENCY), (12, REGISTRATION), (13, PS), (14, SIGNALING), (15, SIGNALING), (17, SIGNALING), (18, SIGNALING), (20, PS), (21, PS))

  private val RRC_ESTB_SUCCESS = "RrcEstbSuccess"
  private val RRC_ESTB_FAILURE = "RrcEstbFailure"

  private val RRC_NORMAL_RELEASE = "RrcNormalRelease"
  private val RRC_DROP = "RrcDrop"

  def normalize(node: JsonNode): Unit = {
    copy(node, node, Some("CommonInfo.CommGenInfo.ConnSetupTime"), CALL + ".StartTime")
    copy(node, node, Some("CommonInfo.CommGenInfo.UeID_PTMSI"), CALL + ".UeID")
    copy(node, node, Some("CommonInfo.CommGenInfo.IMSI"), CALL + ".IMSI")
//    copy(node, node, "CommonInfo.CommGenInfo.IMEI_TAC"+copy(node, node, "CommonInfo.CommGenInfo.IMEI_SNR"+copy(node, node, "CommonInfo.CommGenInfo.IMEI_SP", imei, //Concatenação de Strings
    copy(node, node, Some("CommonInfo.CommGenInfo.CallSetupType"), CALL + ".CallSetupType") //ID, exemplo 4
    copy(node, node, Some("CommonInfo.CommGenInfo.CallSetupTypeLabel"), CALL + ".CallSetupTypeLabel") //String, exemplo "RRC normal establish without DRD"
    copy(node, node, Some("CommonInfo.CommGenInfo.PropgDelay"), CALL + ".PropgDelay")
    copy(node, node, Some("CommonInfo.CommGenInfo.SupOfNoLoss"), CALL + ".SupOfNoLoss")
    copy(node, node, Some("CommonInfo.CommGenInfo.SupOfGsm"), CALL + ".SupOfGsm")
    copy(node, node, Some("CommonInfo.CommGenInfo.SupOfULCMTdd"), CALL + ".SupOfULCMTdd")
    copy(node, node, Some("CommonInfo.CommGenInfo.SupOfULCMGsm"), CALL + ".SupOfULCMGsm")
    copy(node, node, Some("CommonInfo.CommGenInfo.SupOfULCMMulCar"), CALL + ".SupOfULCMMulCar")
    copy(node, node, Some("CommonInfo.CommGenInfo.SupOfDLCMTdd"), CALL + ".SupOfDLCMTdd")
    copy(node, node, Some("CommonInfo.CommGenInfo.SupOfDLCMGsm"), CALL + ".SupOfDLCMGsm")
    copy(node, node, Some("CommonInfo.CommGenInfo.SupOfDLCMMulCar"), CALL + ".SupOfDLCMMulCar")
    copy(node, node, Some("CommonInfo.CommGenInfo.UeRelInd"), CALL + ".UeRelInd") //ID, exemplo 2
    copy(node, node, Some("CommonInfo.CommGenInfo.UeRelIndLabel"), CALL + ".UeRelIndLabel") //String, exemplo "RNCAP_REL_6_ACCESS_STRATUM_REL_IND"
    copy(node, node, Some("CommonInfo.CommGenInfo.UePwrClass"), CALL + ".UePwrClass")
    copy(node, node, Some("CommonInfo.CommGenInfo.HsdschPhyLayerCatg"), CALL + ".HsdschPhyLayerCatg")
    copy(node, node, Some("CommonInfo.CommGenInfo.EdchPhyLayerCatg"), CALL + ".EdchPhyLayerCatg")
    copy(node, node, Some("CommonInfo.CommGenInfo.IMEI_TAC"), CALL + ".TAC")
    copy(node, node, Some("CommonInfo.CommGenInfo.SRNC_ID"), CALL + ".RncID")
    copy(node, node, Some("SingAccess.SigAccTit.bit4RrcConnState"), CALL + ".rcConnState")
//    calltype de acordo com sigcause), CALLtype, //consultar tabela sigcause_calltype abaixo
    copy(node, node, Some("SingAccess.SigAccTit.SigCause"), CALL + ".SigCause") //ID, exemplo 12
    copy(node, node, Some("SingAccess.SigAccTit.SigCauseLabel"), CALL + ".SigCauseLabel") //String, exemplo "UU_REGISTRATION_ESTAB_CAUSE"
    copy(node, node, Some("SingAccess.SigAccTit.SigSetCellId"), CALL + ".RrcSetCellID")
    copy(node, node, Some("SingAccess.SigAccTit.RLSetTime"), CALL + ".RLSetTime")
    copy(node, node, Some("SingAccess.SigAccTit.RLDelTime"), CALL + ".RLDelTime")
    copy(node, node, Some("RrcRelInfo.RrcRelTitInfo.RrcRelTime"), CALL + ".CallEndTime")
    copy(node, node, Some("RrcRelInfo.RrcRelTitInfo.RrcActRNCId"), CALL + ".eRncID")
    copy(node, node, Some("RrcRelInfo.RrcRelTitInfo.RrcActCellId"), CALL + ".eCellID")
    copy(node, node, Some("RrcRelInfo.RrcRelTitInfo.RrcIuCausType"), CALL + ".rrcIuCauseType")
    copy(node, node, Some("RrcRelInfo.RrcRelTitInfo.RrcIuCause"), CALL + ".rrcIuCause")
    copy(node, node, Some("StatInfo.StatSho.Stat1CelLst"), CALL + ".StatsHo1CelLst")
    copy(node, node, Some("StatInfo.StatSho.Stat2CelLst"), CALL + ".StatsHo2CelLst")
    copy(node, node, Some("StatInfo.StatSho.StatShoAtt"), CALL + ".StatShoAtt")
    copy(node, node, Some("StatInfo.StatSho.StatShoSucc"), CALL + ".StatShoSucc")
    copy(node, node, Some("StatInfo.StatSho.StatShoerAtt"), CALL + ".StatShoerAtt")
    copy(node, node, Some("StatInfo.StatSho.StatShoerSucc"), CALL + ".StatShoerSucc")
    copy(node, node, Some("StatInfo.StatDccc.StatDccUpReq"), CALL + ".StatDccUpReq")
    copy(node, node, Some("StatInfo.StatDccc.StatDccUpSucc"), CALL + ".StatDccUpSucc")
    copy(node, node, Some("StatInfo.StatDccc.StatDccDowReq"), CALL + ".StatDccDowReq")
    copy(node, node, Some("StatInfo.StatDccc.StatDccDowSucc"), CALL + ".StatDccDowSucc")
    copy(node, node, Some("StatInfo.StatRab.PsAttSet"), CALL + ".StatRabPsAttSet")
    copy(node, node, Some("StatInfo.StatRab.HAttSet"), CALL + ".StatRabHAttSet")
    copy(node, node, Some("StatInfo.StatRab.PsSuccSet"), CALL + ".StatRabPsSuccSet")
    copy(node, node, Some("StatInfo.StatRab.HSuccSet"), CALL + ".StatRabHSuccSet")
    copy(node, node, Some("StatInfo.StatRab.PsNorRel"), CALL + ".StatRabPsNorRel")
    copy(node, node, Some("StatInfo.StatRab.AmrAbnRel"), CALL + ".StatAmrAbnRelease")
    copy(node, node, Some("StatInfo.StatRab.AmrAttSet"), CALL + ".StatAmrAttSetup")
    copy(node, node, Some("StatInfo.StatRab.AmrNorRel"), CALL + ".StatAmrNormalRelease")
    copy(node, node, Some("StatInfo.StatRab.AmrSuccSet"), CALL + ".StatAmrSuccessSetup")
    copy(node, node, Some("StatInfo.StatRab.HAbnRel"), CALL + ".StatHAbnRelease")
    copy(node, node, Some("StatInfo.StatRab.HNorRel"), CALL + ".StatHNormalRelease")
    copy(node, node, Some("StatInfo.StatRab.PsAbnRel"), CALL + ".StatRabPsAbnRelease")
    copy(node, node, Some("StatInfo.StatRab.bit1VpAttSet"), CALL + ".StatRabVpAttSetup")
    copy(node, node, Some("StatInfo.StatRab.bit1VpNorRel"), CALL + ".StatRabVpNormalRelease")
    copy(node, node, Some("StatInfo.StatRab.bit1VpSuccSet"), CALL + ".StatRabVpSuccessSetup")
    copy(node, node, Some("StatInfo.StatHho.StatIntraHhoReq"), CALL + ".StatIntraHhoReq")
    copy(node, node, Some("StatInfo.StatHho.StatIntraHhoSucc"), CALL + ".StatIntraHhoSucc")
    copy(node, node, Some("StatInfo.StatHho.StatInterHhoReq"), CALL + ".StatInterHhoReq")
    copy(node, node, Some("StatInfo.StatHho.StatInterHhoSucc"), CALL + ".StatInterHhoSucc")
    copy(node, node, Some("SingAccess.SigAbornInfo.SigErrorCause"), CALL + ".RrcSetupErrorCause") //Pode ser mapeado para um string atraves de um lookup
    copy(node, node, Some("SingAccess.SigAbornInfo.SigItfFailDesc"), CALL + ".RrcSetupItfFail") //Pode ser mapeado para um string atraves de um lookup
    copy(node, node, Some("SingAccess.SigAbornInfo.SigFailCauChoice"), CALL + ".RrcSetupCauseChoice") //Pode ser mapeado para um string atraves de um lookup
    copy(node, node, Some("RrcRelInfo.RrcRelAbnInfo.RrcErrorCause"), CALL + ".RrcRelErrorCause") //Pode ser mapeado para um string atraves de um lookup
    copy(node, node, Some("RrcRelInfo.RrcRelAbnInfo.RrcFailCauChoiceLabel"), CALL + ".RrcRelCauseChoice") //Pode ser mapeado para um string atraves de um lookup
    copy(node, node, Some("RrcRelInfo.RrcRelAbnInfo.RrcItfFailDesc"), CALL + ".RrcRelItfFail") //Pode ser mapeado para um string atraves de um lookup

    (findNode(node, Some(IMEI_TAC_NODE)), findNode(node, Some(IMEI_SNR_NODE)), findNode(node, Some(IMEI_SP_NODE))) match {
      case (Some(imeiTAC), Some(imeiSNR), Some(imeiSP)) =>
        add(node, new TextNode(imeiTAC.asText() + imeiSNR.asText() + imeiSP.asText()), CALL + ".IMEI")
      case _ =>
    }

    (findNode(node, Some(CALL + ".StartTime")), findNode(node, Some(CALL + ".CallEndTime"))) match {
      case (Some(callStartTime), Some(callEndTime)) =>
        add(node, new LongNode(callEndTime.asInt - callStartTime.asInt), CALL + ".CallDuration")
      case _ =>
    }

    findNode(node, Some("SigCause")) match {
      case Some(sigCause) => add(node, new TextNode(SIG_CAUSE_MAP(sigCause.asInt())), CALL + ".SigCauseCallType")
      case _ =>
    }

    if (exists(node, Some("SingAccess.SigAbornInfo"))) {
      add(node, BooleanNode.getTrue, CALL + "." + RRC_ESTB_FAILURE)
      add(node, BooleanNode.getFalse, CALL + "." + RRC_ESTB_SUCCESS)
    } else {
      add(node, BooleanNode.getTrue, CALL + "." + RRC_ESTB_SUCCESS)
      add(node, BooleanNode.getFalse, CALL + "." + RRC_ESTB_FAILURE)
    }

    if (exists(node, Some("RrcRelInfo.RrcRelAbnInfo"))) {
      add(node, BooleanNode.getTrue, CALL + "." + RRC_DROP)
      add(node, BooleanNode.getFalse, CALL + "." + RRC_NORMAL_RELEASE)
    } else {
      add(node, BooleanNode.getTrue, CALL + "." + RRC_NORMAL_RELEASE)
      add(node, BooleanNode.getFalse, CALL + "." + RRC_DROP)
    }

//    remove(node, "CommonInfo")
//    remove(node, "RrcRelInfo")
//    remove(node, "RABInfo")
//    remove(node, "StatInfo")
//    remove(node, "SingAccess")
//    remove(node, "ShoInfos")
//    remove(node, "NetOptInfo")
  }
}
