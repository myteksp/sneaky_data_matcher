package com.dataprocessor.server.utils.csv;

import com.dataprocessor.server.utils.StringUtil;
import com.dataprocessor.server.utils.json.JSON;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class CsvUtil {

    public static final CsvIterator parseCsv(final File file, final boolean deleteFileOnClose){
        final Logger logger = LoggerFactory.getLogger("CsvParser");
        final Map.Entry<CSVFormat, Integer> formatAndCount = csvAutodetectAndCountRows(file);
        if (formatAndCount == null){
            logger.error("Unknown csv format.");
            throw new RuntimeException("CSV format unknown.");
        }
        final CSVParser _csvParser;
        try {
            _csvParser = new CSVParser(new BufferedReader(new FileReader(file)), formatAndCount.getKey());
        }catch (final Throwable cause){
            logger.error("Failed to open CSV.", cause);
            throw new RuntimeException("Failed to open CSV.");
        }

        return new CsvIterator() {
            private final long timeStamp = System.currentTimeMillis();
            private final int rowsCount = formatAndCount.getValue();
            private final CSVParser csvParser = _csvParser;
            private final Iterator<CSVRecord> iterator = _csvParser.iterator();
            private long currentRow = 0;

            @Override
            public final long getCurrentRow() {
                return currentRow;
            }

            @Override
            public final long getTotalRows() {
                return rowsCount;
            }

            @Override
            public final Map<String, Integer> getHeaderMap() {
                return csvParser.getHeaderMap();
            }

            @Override
            public final long timeStamp() {
                return timeStamp;
            }

            @Override
            public final List<CsvRecord> getBulk(final int amount) {
                final List<CsvRecord> result = new ArrayList<>(amount);
                for (int i = 0; i < amount; i++) {
                    if (this.hasNext()){
                        result.add(this.next());
                    }else {
                        break;
                    }
                }
                return result;
            }

            @Override
            public final boolean hasNext() {
                return iterator.hasNext();
            }
            @Override
            public final void close() throws IOException {
                csvParser.close();
                if (deleteFileOnClose) {
                    final boolean deleteRes = file.delete();
                    if (!deleteRes) {
                        logger.warn("Failed to delete temp file: '{}'.", file.getAbsolutePath());
                    }
                }
            }
            @Override
            public final CsvRecord next() {
                try {
                    final CSVRecord next = iterator.next();
                    if (next == null)
                        return null;
                    currentRow++;
                    return new CsvRecord() {
                        private final CSVRecord record = next;
                        @Override
                        public final String getColumnVale(final String nameOrIndex) {
                            try{
                                final String val = record.get(nameOrIndex);
                                if (!StringUtil.isNullOrBlank(val)){
                                    return val;
                                }
                            }catch (final Throwable ignored){}
                            try {
                                final int index = Integer.parseInt(nameOrIndex);
                                return record.get(index);
                            }catch (final Throwable ignored){}
                            return null;
                        }
                    };
                }catch (final Throwable ignored){}
                return null;
            }
        };
    }

    private final static Map.Entry<CSVFormat, Integer> csvAutodetectAndCountRows(final File file){
        final Logger logger = LoggerFactory.getLogger("CsvFormatAutodetect");
        final CSVFormat[] formats = new CSVFormat[]{
                CSVFormat.DEFAULT.withHeader(), CSVFormat.MONGODB_CSV.withHeader(), CSVFormat.MONGODB_TSV.withHeader(), CSVFormat.EXCEL.withHeader(), CSVFormat.INFORMIX_UNLOAD.withHeader(), CSVFormat.INFORMIX_UNLOAD_CSV.withHeader(), CSVFormat.TDF.withHeader(), CSVFormat.MYSQL.withHeader(), CSVFormat.ORACLE.withHeader(), CSVFormat.POSTGRESQL_CSV.withHeader(), CSVFormat.POSTGRESQL_TEXT.withHeader(), CSVFormat.RFC4180.withHeader(),
                CSVFormat.DEFAULT, CSVFormat.MONGODB_CSV, CSVFormat.MONGODB_TSV, CSVFormat.EXCEL, CSVFormat.INFORMIX_UNLOAD, CSVFormat.INFORMIX_UNLOAD_CSV, CSVFormat.TDF, CSVFormat.MYSQL, CSVFormat.ORACLE, CSVFormat.POSTGRESQL_CSV, CSVFormat.POSTGRESQL_TEXT, CSVFormat.RFC4180};

        for(final CSVFormat format : formats){
            final CSVParser csvParser;
            try {
                csvParser = new CSVParser(new BufferedReader(new FileReader(file)), format);
                final Iterator<CSVRecord> iterator = csvParser.iterator();
                int counter = 0;
                String[] values = null;
                while (iterator.hasNext()) {
                    final CSVRecord record = iterator.next();
                    counter++;

                    values = record.values();
                }
                final int rowsCount = counter;
                if (values != null){
                    logger.info("CSV format detected: '{}'. Last row: {}. Rows count: {}.", format, JSON.toJson(values), rowsCount);
                }

                return new Map.Entry<>() {
                    @Override
                    public final CSVFormat getKey() {
                        return format;
                    }
                    @Override
                    public final Integer getValue() {
                        return rowsCount;
                    }
                    @Override
                    public final Integer setValue(final Integer value) {
                        throw new RuntimeException("NOT SUPPORTED");
                    }
                };
            } catch (final Throwable ignored) {}
        }
        return null;
    }
    public static interface CsvIterator extends Iterator<CsvRecord>, Closeable {
        long getCurrentRow();
        long getTotalRows();
        Map<String, Integer> getHeaderMap();
        long timeStamp();
        List<CsvRecord> getBulk(final int amount);
    }

    public static interface CsvRecord{
        public String getColumnVale(final String nameOrIndex);

    }
}
