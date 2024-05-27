package com.dataprocessor.server.services;

import com.dataprocessor.server.entities.UploadMapping;
import com.dataprocessor.server.repositories.NormalizedFilesRepository;
import com.dataprocessor.server.utils.StringTransformer;
import com.dataprocessor.server.utils.StringUtil;
import com.dataprocessor.server.utils.TempFileUtil;
import com.dataprocessor.server.utils.csv.CsvUtil;
import com.dataprocessor.server.utils.tuples.Tuple2;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.File;
import java.io.FileWriter;
import java.util.*;

@Service
public final class NormalizedFilesService {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final RecordValidationUtilService recordValidationUtilService;
    private final NormalizedFilesRepository repository;

    @Autowired
    public NormalizedFilesService(final RecordValidationUtilService recordValidationUtilService,
                                  final NormalizedFilesRepository repository){
        this.recordValidationUtilService = recordValidationUtilService;
        this.repository = repository;
    }

    private final Tuple2<CSVPrinter, File> openPrinterFile(final List<UploadMapping> mappings){
        final List<String> headerList = new ArrayList<>(mappings.stream().map(m->m.destinationColumn).toList());
        headerList.add("_ROW_ID");
        final String[] header = headerList.toArray(new String[]{});
        final File outputFile = TempFileUtil.createTmpFile(".csv");
        final CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader(header)
                .build();
        final FileWriter fw;
        try {
            fw = new FileWriter(outputFile);
        }catch (final Throwable cause){
            logger.error("Failed to create file writer for search export.", cause);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to create file writer", cause);
        }
        final CSVPrinter printer;
        try {
            printer = new CSVPrinter(fw, csvFormat);
        }catch (final Throwable cause){
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to create file printer", cause);
        }
        return new Tuple2<>(printer, outputFile);
    }

    public final void normalizeAndServeCsv(final CsvUtil.CsvIterator iterator,
                                           final String uploadName,
                                           final List<UploadMapping> mappings){
        final Tuple2<CSVPrinter, File> output = openPrinterFile(mappings);

        while (iterator.hasNext()){
            final CsvUtil.CsvRecord record = iterator.next();
            final List<Tuple2<String, String>> row = new ArrayList<>(mappings.size());
            boolean addRow = false;
            for(final UploadMapping mapping : mappings){
                final StringBuilder sb = new StringBuilder(128);
                //Iterate over source columns as more than one is allowed (e.g. first and last name)
                for (int i = 0; i < mapping.sourceColumns.size() - 1; i++) {
                    sb.append(recordValidationUtilService.extractAndValidate(record, mapping.sourceColumns.get(i), mapping.transformations)).append(" ");
                }
                sb.append(recordValidationUtilService.extractAndValidate(record, mapping.sourceColumns.getLast(), mapping.transformations));

                final String resultValue = StringTransformer.transform(sb.toString(), mapping.transformations);
                if (StringUtil.isNullOrBlank(resultValue)){
                    row.add(new Tuple2<>(mapping.destinationColumn, ""));
                }else{
                    row.add(new Tuple2<>(mapping.destinationColumn, resultValue));
                    addRow = true;
                }
            }
            if (addRow) {
                row.add(new Tuple2<>("_ROW_ID", StringUtil.generateId()));
                try {
                    output.v1.printRecord(row.stream().map(r -> r.v2).toList());
                } catch (final Throwable t) {
                    logger.warn("Failed to write a row to normalized csv.", t);
                }
            }
        }
        try {
            output.v1.close(true);
        }catch (final Throwable t){
            logger.warn("Failed to close normalized csv printer.", t);
        }
        repository.saveNormalized(uploadName, output.v2);
        output.v2.delete();
    }

    public final File getNormalizedFile(final String name){
        return repository.getNormalized(name);
    }
}
