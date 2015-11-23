package com.airtonjal.poc;

import java.util.Map;

/**
 * Call history record (CHR)
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
public class CallHistoryRecord {

   /**
    * Data inside CHR files is represented as a tree
    */
   private Map<String, Object> dataMap;

   public CallHistoryRecord(Map<String, Object> dataMap) {
      this.dataMap = dataMap;
   }
   public Map<String, Object> getDataMap() {
      return dataMap;
   }

   public void setDataMap(Map<String, Object> dataMap) {
      this.dataMap = dataMap;
   }
}
