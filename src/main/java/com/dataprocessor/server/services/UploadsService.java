package com.dataprocessor.server.services;

import com.dataprocessor.server.entities.UploadDescriptor;
import com.dataprocessor.server.entities.UploadMapping;
import com.dataprocessor.server.repositories.*;
import com.dataprocessor.server.utils.StringTransformer;
import com.dataprocessor.server.utils.StringUtil;
import com.dataprocessor.server.utils.csv.CsvUtil;
import com.dataprocessor.server.utils.tuples.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Service
public final class UploadsService {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final UploadRepository repository;
    private final IndexManager indexManager;
    private final SourceFilesRepository sourceFilesRepository;
    private final RecordValidationUtilService recordValidationUtilService;

    @Autowired
    public UploadsService(final UploadRepository repository,
                          final IndexManager indexManager,
                          final SourceFilesRepository sourceFilesRepository,
                          final RecordValidationUtilService recordValidationUtilService){
        this.repository = repository;
        this.indexManager = indexManager;
        this.sourceFilesRepository = sourceFilesRepository;
        this.recordValidationUtilService = recordValidationUtilService;
    }

    private final int bulkSize = 10;

    public final UploadDescriptor continueIngestion(final String uploadName){
        final UploadDescriptor uploadDescriptor = getUploadDescriptorByName(uploadName);
        if (uploadDescriptor == null){
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Upload with name '" + uploadName + "' not found.");
        }
        if (uploadDescriptor.status != UploadDescriptor.Status.PROCESSING){
            repository.completeUploadWithError(uploadDescriptor);
            logger.warn("Upload status is finished.");
            return null;
        }
        final File file;
        try {
            file = sourceFilesRepository.getSourceFile(uploadName);
        }catch (final Throwable cause){
            repository.completeUploadWithError(uploadDescriptor);
            logger.warn("Source file not found.", cause);
            return null;
        }
        ensureMappingIndexes(uploadDescriptor.mappings);
        final CsvUtil.CsvIterator iterator = CsvUtil.parseCsv(file, true);
        for (long i=0; i < uploadDescriptor.processed; i++){
            if (iterator.hasNext()){
                iterator.next();
            }else {
                try{iterator.close();}catch (final Throwable ignored){}
                throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Upload with name '" + uploadName + "' has wrong size.");
            }
        }
        Thread.startVirtualThread(()->{
            try {
                final List<List<Tuple2<String, String>>> recordsBuffer = new ArrayList<>(bulkSize+16);
                while (iterator.hasNext()){
                    final CsvUtil.CsvRecord record = iterator.next();
                    final List<Tuple2<String, String>> records = new ArrayList<>(uploadDescriptor.mappings.size());
                    for(final UploadMapping mapping : uploadDescriptor.mappings){
                        final StringBuilder sb = new StringBuilder(128);
                        //Iterate over source columns as more than one is allowed (e.g. first and last name)
                        for (int i = 0; i < mapping.sourceColumns.size() - 1; i++) {
                            sb.append(recordValidationUtilService.extractAndValidate(record, mapping.sourceColumns.get(i), mapping.transformations)).append(" ");
                        }
                        sb.append(recordValidationUtilService.extractAndValidate(record, mapping.sourceColumns.getLast(), mapping.transformations));

                        final String resultValue = StringTransformer.transform(sb.toString(), mapping.transformations);
                        if (!StringUtil.isNullOrBlank(resultValue)){
                            records.add(new Tuple2<>(mapping.destinationColumn, resultValue));
                        }
                    }
                    if (recordsBuffer.size() < bulkSize){
                        recordsBuffer.add(records);
                    }else{
                        repository.addRecords(uploadDescriptor, iterator, recordsBuffer);
                        recordsBuffer.clear();
                    }
                }
                repository.addRecords(uploadDescriptor, iterator, recordsBuffer);
                recordsBuffer.clear();
                repository.completeUploadWithSuccess(uploadDescriptor);
            }catch (final Throwable cause){
                logger.warn("Failure while ingesting upload '{}'", uploadName, cause);
                repository.completeUploadWithError(uploadDescriptor);
            }finally {
                try{iterator.close();}catch (final Throwable ignored){}
            }
        });
        return uploadDescriptor;
    }

    public final UploadDescriptor ingest(final File file,
                                         final String uploadName,
                                         final List<UploadMapping> mappings){
        final CsvUtil.CsvIterator iterator = openIterator(file, uploadName);
        ensureMappingIndexes(mappings);
        final UploadDescriptor uploadDescriptor = repository.createUpload(uploadName, mappings, iterator);
        sourceFilesRepository.saveSourceFile(uploadDescriptor.name, file);
        Thread.startVirtualThread(()->{
            try {
                final List<List<Tuple2<String, String>>> recordsBuffer = new ArrayList<>(bulkSize+16);
                while (iterator.hasNext()){
                    final CsvUtil.CsvRecord record = iterator.next();
                    final List<Tuple2<String, String>> records = new ArrayList<>(mappings.size());
                    for(final UploadMapping mapping : mappings){
                        final StringBuilder sb = new StringBuilder(128);
                        //Iterate over source columns as more than one is allowed (e.g. first and last name)
                        for (int i = 0; i < mapping.sourceColumns.size() - 1; i++) {
                            sb.append(recordValidationUtilService.extractAndValidate(record, mapping.sourceColumns.get(i), mapping.transformations)).append(" ");
                        }
                        sb.append(recordValidationUtilService.extractAndValidate(record, mapping.sourceColumns.getLast(), mapping.transformations));

                        final String resultValue = StringTransformer.transform(sb.toString(), mapping.transformations);
                        if (!StringUtil.isNullOrBlank(resultValue)){
                            records.add(new Tuple2<>(mapping.destinationColumn, resultValue));
                        }
                    }
                    if (recordsBuffer.size() < bulkSize){
                        recordsBuffer.add(records);
                    }else{
                        repository.addRecords(uploadDescriptor, iterator, recordsBuffer);
                        recordsBuffer.clear();
                    }
                }
                repository.addRecords(uploadDescriptor, iterator, recordsBuffer);
                recordsBuffer.clear();
                repository.completeUploadWithSuccess(uploadDescriptor);
            }catch (final Throwable cause){
                logger.warn("Failure while ingesting upload '{}'", uploadName, cause);
                repository.completeUploadWithError(uploadDescriptor);
            }finally {
                try{iterator.close();}catch (final Throwable ignored){}
            }
        });
        return uploadDescriptor;
    }

    private final CsvUtil.CsvIterator openIterator(final File file, final String uploadName){
        if (getUploadDescriptorByName(uploadName) != null){
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Upload with name '" + uploadName + "' already exists");
        }
        final CsvUtil.CsvIterator iterator = CsvUtil.parseCsv(file, true);
        if (getUploadDescriptorByName(uploadName) != null){
            try{iterator.close();}catch (final Throwable ignored){}
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Upload with name '" + uploadName + "' already exists");
        }
        return iterator;
    }

    private final void ensureMappingIndexes(final List<UploadMapping> mappings){
        for(final UploadMapping mapping : mappings){
            indexManager.ensureIndex("I" + mapping.destinationColumn + "Index", mapping.destinationColumn, "value");
        }
    }

    public final UploadDescriptor getUploadDescriptorByName(final String name){
        return repository.getUploadByName(name);
    }

    public final List<UploadDescriptor> listUnfinishedUploads(){
        return repository.listUnfinishedUploads();
    }
    public final List<UploadDescriptor> listFinishedUploads(final int skip, final int limit){
        return repository.listFinishedUploads(skip, limit);
    }
}
