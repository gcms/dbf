package org.jamel.dbf.utils;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author Sergey Polovko
 */
public final class DbfUtils {

    private DbfUtils() {
    }

    public static int readLittleEndianInt(DataInput in) throws IOException {
        int bigEndian = 0;
        for (int shiftBy = 0; shiftBy < 32; shiftBy += 8) {
            bigEndian |= (in.readUnsignedByte() & 0xff) << shiftBy;
        }
        return bigEndian;
    }


    public static int readLittleEndianInt(byte[] in) throws IOException {
        DataInput input = new DataInputStream(new ByteArrayInputStream(in));
        return readLittleEndianInt(input);
    }

    public static short readLittleEndianShort(DataInput in) throws IOException {
        int low = in.readUnsignedByte() & 0xff;
        int high = in.readUnsignedByte();
        return (short) (high << 8 | low);
    }

    public static int calcLenTrimLeftSpaces(byte[] arr, int start, int len) {
        int end = start + len;
        while (--end >= start && arr[end] == ' ') /* EMPTY LOOP */ ;

        return end - start + 1;
    }

    public static byte[] trimLeftSpaces(byte[] arr, int start, int len) {
        int trimmedLen = calcLenTrimLeftSpaces(arr, start, len);

        byte[] result = new byte[trimmedLen];
        if (trimmedLen > 0)
            System.arraycopy(arr, start, result, 0, trimmedLen);

        return result;
    }

    public static byte[] trimLeftSpaces(byte[] arr) {
        return trimLeftSpaces(arr, 0, arr.length);
    }

    public static boolean contains(byte[] arr, byte value) {
        for (byte anArr : arr) {
            if (anArr == value) return true;
        }

        return false;
    }

    /**
     * parses only positive numbers
     *
     * @param bytes bytes of string value
     * @return integer value
     */
    public static int parseInt(byte[] bytes) {
        int result = 0;
        for (byte aByte : bytes) {
            if (aByte == ' ') return result;

            result *= 10;
            result += (aByte - (byte) '0');
        }

        return result;
    }

    /**
     * parses only positive numbers
     *
     * @param bytes bytes of string value
     * @param from  index to start from
     * @param to    index to end at
     * @return integer value
     */
    public static int parseInt(byte[] bytes, int from, int to) {
        int result = 0;
        for (int i = from; i < to && i < bytes.length; i++) {
            result *= 10;
            result += (bytes[i] - (byte) '0');
        }
        return result;
    }

    /**
     * parses only positive numbers
     *
     * @param bytes bytes of string value
     * @return long value
     */
    public static long parseLong(byte[] bytes) {
        long result = 0;
        for (byte aByte : bytes) {
            if (aByte == ' ') return result;

            result *= 10;
            result += (aByte - (byte) '0');
        }

        return result;
    }

    /**
     * parses only positive numbers
     *
     * @param bytes bytes of string value
     * @param from  index to start from
     * @param to    index to end at
     * @return integer value
     */
    public static long parseLong(byte[] bytes, int from, int to) {
        long result = 0;
        for (int i = from; i < to && i < bytes.length; i++) {
            result *= 10;
            result += (bytes[i] - (byte) '0');
        }
        return result;
    }
}
