package org.jamel.dbf;

import org.jamel.dbf.exception.DbfException;
import org.jamel.dbf.structure.DbfHeader;
import org.jamel.dbf.structure.DbfRecord;
import org.jamel.dbf.structure.DbfRow;

import java.io.*;
import java.nio.charset.Charset;

/**
 * Dbf reader.
 * This class is not thread safe.
 *
 * @author Sergey Polovko
 * @see <a href="http://www.fship.com/dbfspecs.txt">DBF specification</a>
 */
public class DbfReader implements Closeable {
    protected final byte DATA_ENDED = 0x1A;
    protected final byte DATA_DELETED = 0x2A;

    private final Charset charset;

    private DataInput dataInput;
    private final DbfHeader header;
    private final DbfRecord record;

    public DbfReader(File file) throws DbfException {
        this(file, Charset.defaultCharset());
    }

    public DbfReader(File file, Charset charset) throws DbfException {
        try {
            dataInput = new RandomAccessFile(file, "r");
            header = DbfHeader.read(dataInput);
            skipToDataBeginning();
            this.charset = charset;
            record = new DbfRecord(header);
        } catch (IOException e) {
            throw new DbfException("Cannot open Dbf file " + file, e);
        }
    }

    public DbfReader(InputStream in) throws DbfException {
        this(in, Charset.defaultCharset());

    }

    public DbfReader(InputStream in, Charset charset) throws DbfException {
        try {
            dataInput = new DataInputStream(new BufferedInputStream(in));
            header = DbfHeader.read(dataInput);
            this.charset = charset;
            record = new DbfRecord(header);
            skipToDataBeginning();
        } catch (IOException e) {
            throw new DbfException("Cannot read Dbf", e);
        }
    }

    private void skipToDataBeginning() throws IOException {
        // it might be required to jump to the start of records at times
        int dataStartIndex = header.getHeaderLength() - 32 * (header.getFieldsCount() + 1) - 1;
        if (dataStartIndex > 0) {
            dataInput.skipBytes(dataStartIndex);
        }
    }

    /**
     * @return {@code true} if the reader can seek forward or backward to a specified record index,
     * {@code false} otherwise.
     */
    public boolean canSeek() {
        return dataInput instanceof RandomAccessFile;
    }

    /**
     * Attempt to seek to a specified record index. If successful the record can be read
     * by calling {@link DbfReader#nextRecord()}.
     *
     * @param n The zero-based record index.
     */
    public void seekToRecord(int n) {
        if (!canSeek()) {
            throw new DbfException("Seeking is not supported.");
        }
        if (n < 0 || n >= header.getNumberOfRecords()) {
            throw new DbfException(String.format("Record index out of range [0, %d]: %d",
                    header.getNumberOfRecords(), n));
        }
        long position = header.getHeaderLength() + n * header.getRecordLength();
        try {
            ((RandomAccessFile) dataInput).seek(position);
        } catch (IOException e) {
            throw new DbfException(
                    String.format("Failed to seek to record %d of %d", n, header.getNumberOfRecords()), e);
        }
    }

    public DbfRow nextRow() {
        Object[] record = nextRecord();
        return record == null
                ? null
                : new DbfRow(header, charset, record);
    }


    public boolean nextRecord(DbfRecord record) {
        return nextRecordData(record.getData());
    }

    /**
     * Reads and returns the next row in the Dbf stream
     *
     * @return The next row as an Object array.
     */
    public Object[] nextRecord() {
        if (!nextRecord(record))
            return null;

        Object recordObjects[] = new Object[header.getFieldsCount()];
        for (int i = 0; i < header.getFieldsCount(); i++) {
            recordObjects[i] = record.readFieldValue(header.getField(i));
        }
        return recordObjects;
    }

    public byte[] nextRecordData() {
        byte[] data = new byte[header.getRecordLength() - 1];
        return nextRecordData(data) ? data : null;
    }

    public boolean nextRecordData(byte[] data) {
        try {
            int nextByte;
            do {
                nextByte = dataInput.readByte();
                if (nextByte == DATA_ENDED) {
                    return false;
                } else if (nextByte == DATA_DELETED) {
                    dataInput.skipBytes(header.getRecordLength() - 1);
                }
            } while (nextByte == DATA_DELETED);


            dataInput.readFully(data);
            return true;
        } catch (EOFException e) {
            return false;
        } catch (IOException e) {
            throw new DbfException("Cannot read next record form Dbf file", e);
        }
    }

    /**
     * @return the number of records in the Dbf.
     */
    public int getRecordCount() {
        return header.getNumberOfRecords();
    }

    /**
     * @return Dbf header info.
     */
    public DbfHeader getHeader() {
        return header;
    }

    @Override
    public void close() {
        try {
            // this method should be idempotent
            if (dataInput instanceof Closeable) {
                ((Closeable) dataInput).close();
                dataInput = null;
            }
        } catch (IOException e) {
            // ignore
        }
    }
}
