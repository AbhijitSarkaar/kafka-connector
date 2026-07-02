package com.example.file;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.*;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FileTask extends SourceTask {

    private static final String FILENAME_FIELD = "filename";
    private static final String POSITION_FIELD = "offset_position";
    public static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    private String file_name;
    private InputStream stream;
    private BufferedReader reader = null;
    private char[] buffer;
    private int offset = 0;
    private String topic;
    private final int initialBufferSize = 1024;

    private Long streamOffset;

    public FileTask() {
        buffer = new char[initialBufferSize];
    }

    @Override
    public String version() {
        return "";
    }

    @Override
    public void start(Map<String, String> props) {

        System.out.println(props.get("topic"));
        System.out.println(props.get("tasks.max"));
        System.out.println(props.get("file"));
        System.out.println("FileTask.start()");

        file_name = props.get("file");
        topic = props.get("topic");

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        // initial case
        if(stream == null) {
            try {


                // create a stream to read from file name provided in connector configuration
                stream = new FileInputStream(file_name);

                // fetch offset of file name
                Map<String, Object> offset = context.offsetStorageReader().offset(Map.of(FILENAME_FIELD, file_name));

                // previous offset found
                if(offset != null) {
                    // fetch last recorded offset from context
                    Object lastRecordedOffset = offset.get(POSITION_FIELD);

                    // invalid last recorded offset
                    if(lastRecordedOffset != null && !(lastRecordedOffset instanceof Long))
                        throw new ConnectException("offset position is of incorrect type");

                    // valid last recorded offset
                    if(lastRecordedOffset != null) {
                        long skipLeft = (Long) lastRecordedOffset;
                        while(skipLeft > 0) {
                            try {
                                // skip characters in stream till last recorded offset
                                long skipped = stream.skip(skipLeft);
                                skipLeft -= skipped;
                            } catch (IOException e) {
                            }
                        }
                    }

                    // set offset in stream
                    streamOffset = (lastRecordedOffset != null) ? (Long) lastRecordedOffset : 0L;
                } else {

                    // no previous offset
                    streamOffset = 0L;
                }

                // read bytes into a buffer reader
                reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));

                System.out.println("opened for reading " + file_name);

            } catch(FileNotFoundException e) {
            } catch (IOException e) {
            }
        }

        try {
            BufferedReader readerCopy = reader;
            if(readerCopy == null) return null;

            ArrayList<SourceRecord> records = null;

            int nread = 0;
            while(readerCopy.ready()) {

                // store in buffer array from file
                // store in buffer[offset, buffer_end]
                nread = readerCopy.read(buffer, offset, buffer.length - offset);
                System.out.println("read " + nread + " bytes from " + file_name);

                if(nread > 0) {

                    // move the offset by number of characters read
                    offset += nread;
                    String line;
                    boolean foundOneLine = false;
                    do {
                        // fetch next line from buffer
                        line = extractLine();
                        if(line != null) {
                            foundOneLine = true;
                            // create
                            if(records == null)
                                records = new ArrayList<>();

                            // add a record for one line
                            records.add(
                                    new SourceRecord(
                                            offsetKey(file_name),
                                            offsetValue(streamOffset),
                                            topic,
                                            null,
                                            null,
                                            null,
                                            VALUE_SCHEMA,
                                            line,
                                            System.currentTimeMillis()
                                    )
                            );

                            // write one line read to topic
                            if(records.size() >= 1) return records;
                        }
                    } while(line != null);

                    // increase buffer size
                    if(!foundOneLine && offset == buffer.length) {
                        char[] newbuf = new char[buffer.length * 2];
                        System.arraycopy(buffer, 0, newbuf, 0, buffer.length);
                        buffer = newbuf;
                    }
                }
            }

            return records;
        } catch (Exception e) {}

        return null;
    }

    // record index in buffer array till a newline character from beginning ( <= offset )
    // read line (line_read) from buffer array [0, newline_character_index - 1]
    // remove line_read from buffer
    // move buffer[newline_character_index + 1, buffer.length] to buffer[0, buffer.length - newline_character_index + 1]
    // re position offset by reducing by number of character consumed from current line
    // return line_read

    private String extractLine() {

        // until a new line character '\n'
        int until = -1, newStart = -1;

        // iterate till offset
        // start from beginning of buffer array
        for(int i = 0; i < offset; ++i) {
            // if newline character
            if(buffer[i] == '\n') {
                until = i;
                newStart = i + 1;
                break;
            }
            // if carriage return
            else if(buffer[i] == '\r') {
                if(i + 1 >= offset) {
                    return null;
                }
                until = i;
                newStart = (buffer[i + 1] == '\n') ? i + 2 : i + 1;
                break;
            }
        }

        if(until != -1) {
            // read current line
            String result = new String(buffer, 0, until);

            // removing current line by removing it from 0th index
            // and copying rest of the buffer contents to beginning ( 0th index )
            System.arraycopy(buffer, newStart, buffer, 0, buffer.length - newStart);

            // reduce offset by number of character consumed
            offset = offset - newStart;
            if(streamOffset != null) {

                // increment stream offset by length of line read
                streamOffset += newStart;
            }

            // current line read
            return result;
        } else {
            return null;
        }
    }

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap(FILENAME_FIELD, filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }

    @Override
    public void stop() {
        System.out.println("FileTask.stop()");
    }

}
