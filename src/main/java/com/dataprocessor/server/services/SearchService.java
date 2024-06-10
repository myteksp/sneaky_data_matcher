package com.dataprocessor.server.services;

import com.dataprocessor.server.entities.*;
import com.dataprocessor.server.repositories.ColumnsRepository;
import com.dataprocessor.server.repositories.ExportsRepository;
import com.dataprocessor.server.repositories.SearchRepository;
import com.dataprocessor.server.utils.StringTransformer;
import com.dataprocessor.server.utils.StringUtil;
import com.dataprocessor.server.utils.TempFileUtil;
import com.dataprocessor.server.utils.UploadMappingUtil;
import com.dataprocessor.server.utils.csv.CsvUtil;
import com.dataprocessor.server.utils.json.JSON;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public final class SearchService {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final SearchRepository repository;
    private final ColumnsRepository columnsRepository;
    private final ExportsRepository exportsRepository;

    @Autowired
    public SearchService(final SearchRepository repository,
                         final ColumnsRepository columnsRepository,
                         final ExportsRepository exportsRepository){
        this.repository = repository;
        this.columnsRepository = columnsRepository;
        this.exportsRepository = exportsRepository;
    }

    public final List<Map<String, List<String>>> searchForField(final String column,
                                                                final String query,
                                                                final QueryType queryType,
                                                                final List<String> limitByUploads,
                                                                final List<String> joinByColumn,
                                                                final int maxJoinDepth,
                                                                final int skip,
                                                                final int limit){
        return repository.search(convertQuery(column, query, queryType), LogicalPredicate.AND, limitByUploads, joinByColumn, maxJoinDepth, skip, limit);
    }

    private final List<String> convertQuery(final String column,
                                            final String query,
                                            final QueryType queryType){
        switch (queryType){
            case EQUALS -> {
                return List.of(column + ":" + query);
            }
            case CONTAINS -> {
                return List.of(column + ":>" + query + "<");
            }
            case STARTS_WITH -> {
                return List.of(column + ":" + query + "<");
            }
            case ENDS_WITH -> {
                return List.of(column + ":>" + query);
            }
            default -> throw new RuntimeException("Operation not supported.");
        }
    }

    public final List<Map<String, List<String>>> search(final List<String> columnSearches,
                                                        final LogicalPredicate predicate,
                                                        final List<String> limitByUploads,
                                                        final List<String> joinByColumns,
                                                        final int maxJoinDepth,
                                                        final int skip,
                                                        final int limit){
        return repository.search(columnSearches, predicate, limitByUploads, joinByColumns, maxJoinDepth, skip, limit);
    }

    public final SearchEntity getSearch(final String name){
        return exportsRepository.getSearchEntity(name);
    }

    public final MatchEntity getMatch(final String name){
        return repository.getMatch(name);
    }

    public final MatchEntity forcefullyCompleteMatch(final String name){
        return repository.completeMatchEntity(name);
    }

    public final List<MatchEntity> listMatches(final int skip, final int limit){
        return repository.listMatches(skip, limit);
    }

    public final MatchEntity matchAndExport(final File file,
                                            final List<String> raw_mappings,
                                            final LogicalPredicate predicate,
                                            final List<String> limitByUploads,
                                            final List<String> joinByColumns,
                                            final int maxJoinDepth,
                                            final String exportDestination){
        if (exportsRepository.doesFileExist(exportDestination)){
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Export with name '" + exportDestination + "' already exists");
        }
        exportsRepository.saveSearch(exportDestination, new SearchEntity(raw_mappings, predicate, limitByUploads, joinByColumns, maxJoinDepth));
        final CsvUtil.CsvIterator iterator = CsvUtil.parseCsv(file, true);
        final MatchEntity resultEntity = repository.createMatchEntity(exportDestination, 0, iterator.getTotalRows());
        final List<UploadMapping> mappings = UploadMappingUtil.parse(raw_mappings);
        Thread.startVirtualThread(()->{
            final String[] header = columnsRepository.listAllColumn().toArray(new String[]{});
            final Map<String, Integer> headerMap = new HashMap<>(header.length * 2);
            for (int i = 0; i < header.length; i++) {
                headerMap.put(header[i], i);
            }
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
            try{
                while (iterator.hasNext()){
                    final CsvUtil.CsvRecord record = iterator.next();
                    final List<Tuple2<String, String>> rowToSearch = new ArrayList<>(mappings.size());
                    for(final UploadMapping mapping : mappings){
                        final String rawValue = record.getColumnVale(mapping.sourceColumns.getFirst());
                        if (StringUtil.isNullOrBlank(rawValue))
                            continue;

                        final String value = StringTransformer.transform(rawValue, mapping.transformations);
                        if (StringUtil.isNullOrBlank(value))
                            continue;

                        rowToSearch.add(new Tuple2<>(mapping.destinationColumn, value));
                    }
                    final MatchEntity matchEntity = repository.updateMatchProgress(exportDestination, iterator.getCurrentRow());
                    if (matchEntity.completed){
                        //break while loop because the match was forcefully completed.
                        break;
                    }
                    if (rowToSearch.isEmpty()){
                        continue;
                    }
                    //search the row
                    final List<Map<String, List<String>>> searchResults = search(rowToSearch.stream().map(t-> t.v1 + ":" + t.v2 + ":tlc:nrm").toList(), predicate, limitByUploads, joinByColumns, maxJoinDepth, 0, 1);
                    //store search results into output csv
                    if (!searchResults.isEmpty()){
                        for (final Map<String, List<String>> rcd : searchResults) {
                            final List<String> sortedRecord = new ArrayList<>(headerMap.size());
                            for (int i = 0; i < headerMap.size(); i++) {
                                sortedRecord.add("");
                            }
                            rcd.remove("_id");
                            for(final Map.Entry<String, List<String>> e : rcd.entrySet()){
                                final List<String> val = e.getValue();
                                if (val.isEmpty()){
                                    sortedRecord.set(headerMap.get(e.getKey()), "");
                                }else if (val.size() == 1){
                                    sortedRecord.set(headerMap.get(e.getKey()), val.getFirst());
                                }else{
                                    sortedRecord.set(headerMap.get(e.getKey()), JSON.toJson(e.getValue()));
                                }
                            }
                            printer.printRecord(sortedRecord);
                        }
                    }
                }
                repository.completeMatchEntity(exportDestination);
            }catch (final Throwable cause){
                logger.warn("Failure while matching '{}'", exportDestination, cause);
            }finally {
                try{iterator.close();}catch (final Throwable ignored){}
                try{printer.close();}catch (final Throwable ignored){}
                try{fw.close();}catch (final Throwable ignored){}
            }
            exportsRepository.saveFile(exportDestination, outputFile);
        });
        return resultEntity;
    }

    public final void searchAndExport(final List<String> columnSearches,
                                      final LogicalPredicate predicate,
                                      final List<String> limitByUploads,
                                      final List<String> joinByColumns,
                                      final int maxJoinDepth,
                                      final String exportDestination){
        if (exportsRepository.doesFileExist(exportDestination)){
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Export with name '" + exportDestination + "' already exists");
        }
        exportsRepository.saveSearch(exportDestination, new SearchEntity(columnSearches, predicate, limitByUploads, joinByColumns, maxJoinDepth));
        Thread.startVirtualThread(()->{
            final String[] header = columnsRepository.listAllColumn().toArray(new String[]{});
            final Map<String, Integer> headerMap = new HashMap<>(header.length * 2);
            for (int i = 0; i < header.length; i++) {
                headerMap.put(header[i], i);
            }

            final File outputFile = TempFileUtil.createTmpFile(".csv");
            int skip = 0;
            final int pageSize = 50;

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

            try (final CSVPrinter printer = new CSVPrinter(fw, csvFormat)) {
                for (;;) {
                    final List<Map<String, List<String>>> batch = search(columnSearches, predicate, limitByUploads, joinByColumns, maxJoinDepth, skip, pageSize);
                    skip += pageSize;
                    if (batch.isEmpty())
                        break;

                    for (final Map<String, List<String>> record : batch) {
                        final List<String> sortedRecord = new ArrayList<>(headerMap.size());
                        for (int i = 0; i < headerMap.size(); i++) {
                            sortedRecord.add("");
                        }
                        record.remove("_id");
                        for(final Map.Entry<String, List<String>> e : record.entrySet()){
                            final List<String> val = e.getValue();
                            if (val.isEmpty()){
                                sortedRecord.set(headerMap.get(e.getKey()), "");
                            }else if (val.size() == 1){
                                sortedRecord.set(headerMap.get(e.getKey()), val.getFirst());
                            }else{
                                sortedRecord.set(headerMap.get(e.getKey()), JSON.toJson(e.getValue()));
                            }
                        }
                        printer.printRecord(sortedRecord);
                    }
                    logger.info("Exported {} rows.", skip);
                }
            }catch (final Throwable cause){
                logger.error("Failed to write csv", cause);
            }finally {
                try {
                    fw.close();
                }catch (final Throwable cause){
                    logger.error("Failed to close file writer", cause);
                }
            }

            exportsRepository.saveFile(exportDestination, outputFile);
        });
    }

    public final List<String> getMappings(){
        return columnsRepository.listAllColumn();
    }
}
