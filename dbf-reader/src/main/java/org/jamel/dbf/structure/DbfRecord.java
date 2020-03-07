package org.jamel.dbf.structure;

import org.jamel.dbf.exception.DbfException;
import org.jamel.dbf.utils.DbfUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * Represents a DBF record
 *
 * @author Gustavo Sousa
 */
public class DbfRecord {
    private byte[] data;

    public DbfRecord(DbfHeader header) {
        this.data = new byte[header.getRecordLength() - 1];
    }

    public byte[] getData() {
        return data;
    }

    public Object readFieldValue(DbfField field) {
        switch (field.getDataType()) {
            case CHAR:
                return readCharacterValue(field);
            case DATE:
                return readDateValue(field);
            case FLOAT:
                return readFloatValue(field);
            case LOGICAL:
                return readLogicalValue(field);
            case NUMERIC:
                return readNumericValue(field);
            case MEMO:
                return readMemoLink(field);
            default:
                return null;
        }
    }

    public byte[] readFieldData(DbfField field) {
        return Arrays.copyOfRange(data, field.getFieldOffset(), field.getFieldOffset() + field.getFieldLength());
    }

    public String readString(int offset, int length, Charset charset) {
        int newLength = DbfUtils.calcLenTrimLeftSpaces(data, offset, length);
        return new String(data, offset, newLength, charset);
    }

    public String readString(DbfField field, Charset charset) {
        return readString(field.getFieldOffset(), field.getFieldLength(), charset);
    }

    public Object readCharacterValue(DbfField field) {
        return readFieldData(field);
    }

    public Date readDateValue(DbfField field) {
        int from = field.getFieldOffset();
        int year = DbfUtils.parseInt(data, from + 0, from + 4);
        int month = DbfUtils.parseInt(data, from + 4, from + 6);
        int day = DbfUtils.parseInt(data, from + 6, from + 8);
        return new GregorianCalendar(year, month - 1, day).getTime();
    }

    public Float readFloatValue(DbfField field) {
        try {
            return readFloatValue(field.getFieldOffset(), field.getFieldLength());
        } catch (NumberFormatException e) {
            throw new DbfException("Failed to parse Float from " + field.getName(), e);
        }
    }

    public Float readFloatValue(int offset, int length) {
        byte[] floatBuf = DbfUtils.trimLeftSpaces(data, offset, length);
        boolean processable = (floatBuf.length > 0 && !DbfUtils.contains(floatBuf, (byte) '?'));
        return processable ? Float.valueOf(new String(floatBuf)) : null;
    }

    public Boolean readLogicalValue(DbfField field) {
        return readLogicalValue(field.getFieldOffset());
    }

    public Boolean readLogicalValue(int offset) {
        byte v = data[offset];
        boolean isTrue = (v == 'Y' || v == 'y' || v == 'T' || v == 't');
        return isTrue ? Boolean.TRUE : Boolean.FALSE;
    }


    public Number readNumericValue(DbfField field) {
        try {
            return readNumericValue(field.getFieldOffset(), field.getFieldLength());
        } catch (NumberFormatException e) {
            throw new DbfException("Failed to parse Number from " + field.getName(), e);
        }
    }

    public Number readNumericValue(int offset, int length) {
        byte[] numericBuf = DbfUtils.trimLeftSpaces(data, offset, length);
        boolean processable = numericBuf.length > 0 && !DbfUtils.contains(numericBuf, (byte) '?');
        return processable ? Double.valueOf(new String(numericBuf)) : null;
    }

    public Number readMemoLink(DbfField field) {
        switch (field.getFieldLength()) {
            case 4:
                try {
                    return DbfUtils.readLittleEndianInt(readFieldData(field));
                } catch (IOException e) {
                    throw new DbfException("Failed to parse MemoLink from " + field.getName(), e);
                }
            case 10:
                return readNumericValue(field);
            default:
                throw new DbfException("Unknown MEMO mode: " + field.getFieldLength());
        }
    }
}
