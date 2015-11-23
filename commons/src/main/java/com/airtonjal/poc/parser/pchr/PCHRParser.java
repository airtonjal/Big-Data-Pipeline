package com.airtonjal.poc.parser.pchr;

import com.airtonjal.poc.CallHistoryRecord;
import com.airtonjal.poc.utils.NibbleUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * PCHR parser interface. This parser outputs JSON that will probably be inserted into ElasticSearch
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
public class PCHRParser {

   private static final Log log = LogFactory.getLog(PCHRParser.class);

   private static final String LEAF_NODE_MARKER = ":";
   private static final String START_MARKER     = "---------version:";
   private static final String END_MARKER       = "-----------------";
   private static final String NULL             = "NULL";
   private static final String TRUE             = "TRUE";
   private static final String FALSE            = "FALSE";
   private static final int    MARK_SIZE        = 2000;
   private static final String LABEL_NAME       = "Label";
   private static final String SHO_INFO_NAME    = "ShoInfo";

   // Map to indicate to serialize fields as specific types
   private final Set<String> stringFields = new HashSet<>();
   private final Set<String> doubleFields = new HashSet<>();
   private final Set<String> intFields    = new HashSet<>();

   public PCHRParser() {
      // Some of the below values are actually hexadecimal, but we store as  String to make it easier

      // Proprietary
      stringFields.add("IMSI");
      stringFields.add("SysHoPlmnMcc");
      stringFields.add("PeerISDN");
      stringFields.add("IMEI_SNR");
      stringFields.add("IMEI_TAC");

      intFields.add("CellId");
      intFields.add("Act1ACellId");
      intFields.add("Mon1ACellId");
      intFields.add("ConnNCellId");
      intFields.add("RrcActCellId");
      intFields.add("RrcRFCellId");
      intFields.add("ShoActCellId");
      intFields.add("ShoMonCellId");
      intFields.add("ShoTarCellId");
      intFields.add("Act1CCellId");
      intFields.add("Mon1CCellId");
      intFields.add("Act1ACellId");
      intFields.add("Mon1ACellId");

      intFields.add("RrcRFRscp");
      intFields.add("SigSetRscp");
      intFields.add("ShoActCelRscp");
      intFields.add("ShoMonCelRscp");
      intFields.add("ActCel1CRscp");
      intFields.add("MonCel1CRscp");
      intFields.add("ActCel1ARscp");
      intFields.add("MonCel1ARscp");
      intFields.add("ConnNCellRscp");

      // Agnostic
      stringFields.add("RrcMsgKeyIe");

      doubleFields.add("SigSetEc_N0");
      doubleFields.add("ConnNCellEc_N0");
      doubleFields.add("RrcRFEc_N0");
      doubleFields.add("ShoMonCelEc_N0");
      doubleFields.add("ShoActCelEc_N0");
      doubleFields.add("HhoTarCelEc_N0");
      doubleFields.add("ActCel1CEc_N0");
      doubleFields.add("MonCel1CEc_N0");
      doubleFields.add("ActCel1AEc_N0");
      doubleFields.add("MonCel1AEc_N0");
   }

   public List<CallHistoryRecord> parseList(File file) throws IOException {
      log.info("Parsing file " + file.getName() + "\tFile size: " + NibbleUtils.readableFileSize(file.length()));

      try (InputStream inputStream = getInputStream(file)) {
         List<CallHistoryRecord> pchrRecords = new ArrayList<>();
         Map<String, Object> eventMap;

         BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

         while((eventMap = fillMap(reader)) != null)
            pchrRecords.add(new CallHistoryRecord(eventMap));

         return pchrRecords;
      }
   }

   private Map<String, Object> fillMap(BufferedReader reader) throws IOException {
      String startLine = reader.readLine();
      if (startLine == null) return null;

      if (!startLine.startsWith(START_MARKER))
         throw new IOException("ERROR. Either the format is wrong or parser has a bug! Expected line should start with " + START_MARKER);

      Map<String, Object> eventMap = fillMap(reader, new HashMap<String, Object>(), -1);

      reader.readLine();

      return eventMap;
   }

   private Map<String, Object> fillMap(BufferedReader reader, Map<String, Object> map, int level) throws IOException {
      reader.mark(MARK_SIZE);
      String line = reader.readLine();
      if (line.startsWith(END_MARKER)) return map;

      do {
         // Used to read a line twice. Better explained bellow
         boolean forceReread = false;

         int nextLevel = countLevel(line);
         if (nextLevel <= level) {
            reader.reset();  // Points the stream back to the node in order to set the recursion to the right level
            return map;
         }

         if (!line.contains(LEAF_NODE_MARKER)) { // Non-leaf node
            line = line.trim();

            if (line.contains(" ")) { // Found a collection
               reader.reset();

               String[] parts = line.split(" ");
               int index = Integer.parseInt(parts[1]);

               if (index == 1) { // First collection member found
                  List<Map<String, Object>> list = fillList(reader);

                  line = parts[0] + "s";  // Uses plural as the property name

                  map.put(line, list);
               }
            } else {
               Map<String, Object> childMap = new HashMap<>();

               // Forcing ShoInfo node to be a list (might have multiple soft handovers)
               if (line.equals(SHO_INFO_NAME)) {
                  List<Map<String, Object>> shoList;
                  if (map.containsKey(SHO_INFO_NAME + "s"))
                     shoList = (List<Map<String, Object>>)map.get(SHO_INFO_NAME + "s");
                  else
                     shoList = new ArrayList<>();

                  shoList.add(childMap);
                  map.put(SHO_INFO_NAME + "s", shoList);
               } else
                  map.put(line, childMap);

               reader.mark(MARK_SIZE);
               fillMap(reader, childMap, nextLevel);
               if (childMap.size() == 0) forceReread = true; // The map didn't have any changes, inner node was same level
            }
         } else {
            addLeaf(line, map);
         }

         // Marks the stream to go back when non-leaf level decreases
         reader.mark(MARK_SIZE);

         // This is only added because sometimes a node comes empty (like NetOptInfo)
         line = reader.readLine();
         if (forceReread) line = reader.readLine();
      } while (!line.startsWith(END_MARKER));

      reader.reset();

      return map;
   }

   private List<Map<String, Object>> fillList(BufferedReader reader) throws IOException {
      String line = reader.readLine();

      String listName = line.trim().split(" ")[0];

      int level = countLevel(line);
      List<Map<String, Object>> list = new ArrayList<>();

      int nextLevel;

      // Used to control empty list elements
      String last = line;
      do {  // When the list finishes, buffer points to an outer record
         list.add(fillMap(reader, new HashMap<String, Object>(), level));

         reader.mark(MARK_SIZE);
         line = reader.readLine();
         if (line.equals(last)) { // Something wrong, read the same line as before, forces reading again
            reader.mark(MARK_SIZE);
            line = reader.readLine();
         }

         last = line;

         nextLevel = countLevel(line);
      } while(level <= nextLevel && line.trim().contains(" ") && !line.contains(":") && line.trim().startsWith(listName));

      reader.reset();

      return list;
   }

   private int countLevel(String nonLeafLine) {
//      if (nonLeafLine.charAt(0) != ' ') return -1;
      for(int i = 0; i < nonLeafLine.length(); i++) {
         if (nonLeafLine.charAt(i) == ' ') continue;
         return i;
      }
      return nonLeafLine.length();
   }

   private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
   private static final DateFormat dateFormat = new SimpleDateFormat(DATE_PATTERN);
   private void addLeaf(String line, Map<String, Object> map) throws IOException {
      line = line.trim();
      int position = line.indexOf(':');

      if (position == line.length() - 1) { // No data after ':' char
         log.trace("Line '" + line + "' seems weird" );
         return;
      }

      String key = line.substring(0, position).trim();
      String value = line.substring(position + 2, line.length()).trim();

      if (key.endsWith(LABEL_NAME))
         map.put(key, value);
      else if (intFields.contains(key))
         tryAsInt(key, value, map);
      else if (doubleFields.contains(key))
         tryAsInt(key, value, map);
      else if (TRUE.equals(value))
         map.put(key, true);
      else if (FALSE.equals(value))
         map.put(key, false);
      else if (stringFields.contains(key))
         map.put(key, value);
      else if (NULL.equals(value.toUpperCase()))
         map.put(key, null);
      else if (key.endsWith("Time"))
         addDate(key, value, map);
      else if (tryAsInt(key, value, map))
         return;
      else if (tryAsDouble(key, value, map))
         return;
      else // Insert as string, don' care about type
         map.put(key, value);
   }

//   private final DateFormat durationFormat = new SimpleDateFormat("HH:mm:ss.SSS", Locale.getDefault());
   private void addDate(String key, String value, Map<String, Object> map) {
      try {
         Date date = dateFormat.parse(value);
         map.put(key, date);
      } catch(ParseException pe) {
         log.trace("Could not recognize date format \'" + value + "\'. Adding as string");
         map.put(key, value);
      }
   }

   private boolean tryAsInt(String key, String value, Map<String, Object> map) {
      try {
         Integer ivalue = Integer.parseInt(value);
         map.put(key, ivalue.intValue());
         return true;
      } catch (NumberFormatException nfe) {}
      return false;
   }

   private boolean tryAsDouble(String key, String value, Map<String, Object> map) {
      try {
         Double dvalue = Double.parseDouble(value);
         map.put(key, dvalue.doubleValue());
         return true;
      } catch (NumberFormatException nfe) {}
      return false;
   }

   public InputStream getInputStream(File file) throws IOException {
      return new FileInputStream(file);
   }
}
