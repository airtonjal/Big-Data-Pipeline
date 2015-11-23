package com.airtonjal.poc.utils;

import java.text.DecimalFormat;

/**
 * File size {@link String} formatter
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
public class SizeFormatter {

   /**
    * Formats a number in a readable file size
    * @param size The file size, in bytess
    * @return A {@link String} with the file size in a readable manner
    */
   public static String readableFileSize(long size) {
      if(size <= 0) return "0";
      final String[] units = new String[] { "B", "KB", "MB", "GB", "TB" };
      int digitGroups = (int) (Math.log10(size)/Math.log10(1024));
      return new DecimalFormat("#,##0.#").format(size/Math.pow(1024, digitGroups)) + " " + units[digitGroups];
   }
}
