package com.airtonjal.poc.utils;

import java.text.DecimalFormat;

/**
 * Manipulation and operations nibbles
 * @author <a href="mailto:airtonjal@gmail.com">Airton Libório</a>
 */
public class NibbleUtils {

   /** Most significant bits mask for binary operations */
   public static final int MOST_SIGNIFICANT_MASK  = 0xFF;
   /** Least significant bits mask for binary operations */
   public static final int LEAST_SIGNIFICANT_MASK = 0x0F; // 15, or 0b00001111

   /** {@code char[]} in which each position contains the hex value textual representation */
   protected static final char[] hexArray = "0123456789ABCDEF".toCharArray();

   public NibbleUtils() {}

   /**
    * Extracts the 4 least significant bits
    * @param b The byte
    * @return A byte with 0b0000 plus the 4 least significant bits
    */
   public static byte leastSignificantNibble(byte b) {
      return (byte)(b & LEAST_SIGNIFICANT_MASK);
   }

   /**
    * Extracts the 4 most significant bits
    * @param b The byte
    * @return A byte with 0b0000 plus the 4 most significant bits
    */
   public static byte mostSignificantNibble(byte b)  {
      return (byte)((b & MOST_SIGNIFICANT_MASK) >>> 4);
   }

   /**
    * Shifts the nibble pair positions in a {@code byte}
    * @param b The encoded {@code byte}
    * @return A {@code byte} with shifted nibbles
    */
   public static byte shiftNibbles(byte b) {
      // Acquires the least significant half and shifts right
      byte leastSignificantHalf = (byte)(leastSignificantNibble(b) << 4);
      byte mostSignificantHalf  = mostSignificantNibble(b);

      return (byte)(leastSignificantHalf | mostSignificantHalf);
   }

   /**
    * Shifts every nibble pair of every byte
    * @see {@link NibbleUtils#shiftNibbles(byte)}
    * @param bytes The byte array
    * @return A {@code byte[]} with each byte in the same position with nibbles reversed
    */
   public static byte[] shiftNibbles(byte[] bytes) {
      assert(bytes != null);   assert(bytes.length > 0);
      byte[] bytesRet = new byte[bytes.length];
      for (int i = 0; i < bytes.length; i++)
         bytesRet[i] = shiftNibbles(bytes[i]);
      return bytesRet;
   }

   /**
    * Shifts nibble pairs of the specified
    * @see {@link NibbleUtils#shiftNibbles(byte)}
    * @param bytes The byte array
    * @param start Start index, inclusive (first index is 1)
    * @param end The end index, inclusive (last index is length)
    * @return A {@code byte[]} with specified bytes reversed. Bytes order are kept
    */
   public static byte[] shiftNibbles(byte[] bytes, int start, int end) {
      assert(bytes != null);   assert(bytes.length > 0);
      byte[] bytesRet = new byte[bytes.length];

      // Only shifts specified bytes
      for (int i = 0; i < bytes.length; i++)
         if (i >= start -1 && i < end)
            bytesRet[i] = shiftNibbles(bytes[i]);
         else
            bytesRet[i] = bytes[i];

      return bytesRet;
   }

   public static String bytesToHex(byte b) {
      return bytesToHex(new byte[] {b} );
   }

   /**
    * Converts each byte nibble to the equivalent hex representation, in a char
    * @param bytes The bytes
    * @return A {@link String} containing the hex representation of each nibble
    */
   public static String bytesToHex(byte[] bytes) {
      char[] hexChars = new char[bytes.length * 2];
      for (int j = 0; j < bytes.length; j++) {
         int v = bytes[j] & MOST_SIGNIFICANT_MASK;
         hexChars[j * 2] = hexArray[v >>> 4];
         hexChars[j * 2 + 1] = hexArray[v & LEAST_SIGNIFICANT_MASK];
      }
      return new String(hexChars);
   }

   /**
    * Converts each {@code char} in the {@link String} to the equivalent hexadecimal value
    * @param str The {@link String}. Every char must be either a number or A - F chars
    * @return A {@code byte[]}, if possible. Each hex value is stored in a nibble, same positions as the {@link String}.
    * If the provided {@link String} contains an even number of chars, 0xF is stored in the last nibble
    */
   public static byte[] hexToBytes(String str) {
      if (str == null) return null;

      // Needs and even number of digits
      if (str.length() % 2 != 0) str += "F";

      int size = str.length();
      byte[] data = new byte[size / 2];

      for (int i = 0; i < size; i += 2) {
         char c1 = Character.toUpperCase(str.charAt(i));
         char c2 = Character.toUpperCase(str.charAt(i + 1));

         // Sets nibbles
         byte b = (byte)(Character.digit(c1, 16) << 4);
         b     += (byte)(Character.digit(c2, 16));

         data[i / 2] = b;
      }

      return data;
   }

   /**
    * Converts a pair of bytes to a {@code short}
    * @param mostSignificant The most significant number part
    * @param leastSignificant The least significant number part
    * @return A {@code short}
    */
   public static short toShort(byte mostSignificant, byte leastSignificant) {
      short n = 0;
      n ^= mostSignificant & 0xFF;
      n <<= 8;
      n ^= leastSignificant & 0xFF;
      return n;
   }

   /**
    * Converts a pair of bytes to a {@code int}
    * @param mostSignificant The most significant number part
    * @param leastSignificant The least significant number part
    * @return A {@code int}
    */
   public static int toInt(byte mostSignificant, byte leastSignificant) {
      int n = 0;
      n ^= mostSignificant & 0xFF;
      n <<= 8;
      n ^= leastSignificant & 0xFF;
      return n;
   }

   /**
    * Gets each nibbles as a decimal value and returns as a {@code short}
    * Example:
    *
    * 0b10001001 = 137
    *
    * Most significant nibble  = 1000 = 8
    * Least significant nibble = 1001 = 9
    *
    * The decimal returned will be 0b1000 * 10 + 0b1001 * 1, or 89
    *
    * @param b The byte
    * @return A decimal value
    */
   public static int getNibbles(byte b) {
      return mostSignificantNibble(b) * 10 + leastSignificantNibble(b);
   }

   // TODO: test eventually or comment
   private static char[] byteToHex(byte b) {
      char[] hexChars = new char[2];
      int v = b & MOST_SIGNIFICANT_MASK;
      hexChars[0] = hexArray[v >>> 4];
      hexChars[1] = hexArray[v & LEAST_SIGNIFICANT_MASK];
      return hexChars;
   }

   /** Just for debugging and testing purposes */
   public static String toBinary(byte b) {
      return String.format("%8s", Integer.toBinaryString(b & MOST_SIGNIFICANT_MASK)).replace(' ', '0');
   }

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
