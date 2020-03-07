package org.jamel.dbf.structure;

import org.jamel.dbf.exception.DbfException;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Date;

import static java.lang.String.format;
import static org.jamel.dbf.utils.DbfUtils.trimLeftSpaces;

/**
 * Represents a DBF row (record) with ability to get field's value by its name.
 *
 * @author Kirill Fertikov
 */
public class DbfRow {

    private static final Long ZERO = 0L;

    private final DbfHeader header;
    private final Charset defaultCharset;
    private final Object[] row;

    public DbfRow(DbfHeader header, Charset defaultCharset, Object[] row) {
        this.header = header;
        this.defaultCharset = defaultCharset;
        this.row = row;
    }

    public DbfRow(DbfHeader header, Charset defaultCharset, DbfRecord record) {
        this.header = header;
        this.defaultCharset = defaultCharset;
        row = new Object[header.getFieldsCount()];
        for (int i = 0; i < header.getFieldsCount(); i++)
            row[i] = record.readFieldValue(header.getField(i));
    }

    /**
     * Retrieves the value of the designated field as java.math.BigDecimal.
     *
     * @param fieldName the name of the field
     * @return the field value, or null (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public BigDecimal getBigDecimal(String fieldName) throws DbfException {
        Object value = get(fieldName);
        return value == null ? null : new BigDecimal(value.toString());
    }

    public BigDecimal getBigDecimal(int fieldIndex) {
        Object value = get(fieldIndex);
        return value == null ? null : new BigDecimal(value.toString());
    }

    public BigDecimal getBigDecimal(DbfField field) {
        Object value = get(field);
        return value == null ? null : new BigDecimal(value.toString());
    }

    /**
     * Retrieves the value of the designated field as java.util.Date.
     *
     * @param fieldName the name of the field
     * @return the field value, or null (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public Date getDate(String fieldName) throws DbfException {
        Date value = (Date) get(fieldName);
        return value == null ? null : value;
    }

    public Date getDate(int fieldIndex) throws DbfException {
        Date value = (Date) get(fieldIndex);
        return value == null ? null : value;
    }


    public Date getDate(DbfField field) throws DbfException {
        Date value = (Date) get(field);
        return value == null ? null : value;
    }

    private String getString(Object value, Charset charset) {
        return value == null
                ? null
                : new String(trimLeftSpaces((byte[]) value), charset);
    }

    /**
     * Retrieves the value of the designated field as String.
     *
     * @param fieldName the name of the field
     * @return the field value, or null (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public String getString(String fieldName) throws DbfException {
        return getString(fieldName, defaultCharset);
    }

    /**
     * Retrieves the value of the designated field as String
     * using given charset.
     *
     * @param fieldName the name of the field
     * @param charset the charset to be used to decode field value
     * @return the field value, or null (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public String getString(String fieldName, Charset charset) throws DbfException {
        return getString(get(fieldName), charset);
    }

    public String getString(int fieldIndex) throws DbfException {
        return getString(fieldIndex, defaultCharset);
    }

    public String getString(int fieldIndex, Charset charset) throws DbfException {
        return getString(get(fieldIndex), charset);
    }

    public String getString(DbfField field, Charset charset) throws DbfException {
        return getString(get(field), charset);
    }


    public String getString(DbfField field) throws DbfException {
        return getString(field, defaultCharset);
    }

    /**
     * Retrieves the value of the designated field as boolean.
     *
     * @param fieldName the name of the field
     * @return the field value, or false (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public boolean getBoolean(String fieldName) throws DbfException {
        Boolean value = (Boolean) get(fieldName);
        return value != null && value;
    }

    public boolean getBoolean(int fieldIndex) throws DbfException {
        Boolean value = (Boolean) get(fieldIndex);
        return value != null && value;
    }

    public boolean getBoolean(DbfField field) throws DbfException {
        Boolean value = (Boolean) get(field);
        return value != null && value;
    }

    /**
     * Retrieves the value of the designated field as int.
     *
     * @param fieldName the name of the field
     * @return the field value, or 0 (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public int getInt(String fieldName) throws DbfException {
        return getNumber(fieldName).intValue();
    }

    /**
     * Retrieves the value of the designated field as short.
     *
     * @param fieldName the name of the field
     * @return the field value, or 0 (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public short getShort(String fieldName) throws DbfException {
        return getNumber(fieldName).shortValue();
    }

    /**
     * Retrieves the value of the designated field as byte.
     *
     * @param fieldName the name of the field
     * @return the field value, or 0 (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public byte getByte(String fieldName) throws DbfException {
        return getNumber(fieldName).byteValue();
    }

    /**
     * Retrieves the value of the designated field as long.
     *
     * @param fieldName the name of the field
     * @return the field value, or 0 (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public long getLong(String fieldName) throws DbfException {
        return getNumber(fieldName).longValue();
    }

    /**
     * Retrieves the value of the designated field as float.
     *
     * @param fieldName the name of the field
     * @return the field value, or 0 (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public float getFloat(String fieldName) throws DbfException {
        return getNumber(fieldName).floatValue();
    }

    /**
     * Retrieves the value of the designated field as double.
     *
     * @param fieldName the name of the field
     * @return the field value, or 0 (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public double getDouble(String fieldName) throws DbfException {
        return getNumber(fieldName).doubleValue();
    }

    /**
     * Retrieves the value of the designated field as Object.
     *
     * @param fieldName the name of the field
     * @return the field value, or null (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public Object getObject(String fieldName) throws DbfException {
        return get(fieldName);
    }

    private Number getNumber(String fieldName) {
        Number value = (Number) get(fieldName);
        return value == null ? ZERO : value;
    }

    private Object get(String fieldName) {
        try {
            int fieldIndex = header.getFieldIndex(fieldName);
            return get(fieldIndex);
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new DbfException(format("Field \"%s\" does not exist", fieldName), e);
        }
    }

    private Object get(DbfField field) {
        return get(field.getFieldIndex());
    }

    private Object get(int fieldIndex) {
        return row[fieldIndex];
    }
}
