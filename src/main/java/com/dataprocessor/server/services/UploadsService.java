package com.dataprocessor.server.services;

import com.dataprocessor.server.entities.UploadDescriptor;
import com.dataprocessor.server.entities.UploadMapping;
import com.dataprocessor.server.repositories.ColumnsRepository;
import com.dataprocessor.server.repositories.IndexManager;
import com.dataprocessor.server.repositories.SourceFilesRepository;
import com.dataprocessor.server.repositories.UploadRepository;
import com.dataprocessor.server.utils.StringTransformer;
import com.dataprocessor.server.utils.StringUtil;
import com.dataprocessor.server.utils.csv.CsvUtil;
import com.dataprocessor.server.utils.tuples.Tuple2;
import org.apache.commons.validator.routines.EmailValidator;
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
    private final ColumnsRepository columnsRepository;

    @Autowired
    public UploadsService(final UploadRepository repository,
                          final IndexManager indexManager,
                          final SourceFilesRepository sourceFilesRepository,
                          final ColumnsRepository columnsRepository){
        this.repository = repository;
        this.indexManager = indexManager;
        this.sourceFilesRepository = sourceFilesRepository;
        this.columnsRepository = columnsRepository;
    }

    private final String extractAndValidate(final CsvUtil.CsvRecord record,
                                            final String rowName,
                                            final List<StringTransformer.Transformation> transformations){
        final String rowNameLowerCase = rowName.toLowerCase();
        final String result = StringTransformer.transform(record.getColumnVale(rowName), transformations);
        if (rowNameLowerCase.contains("mail")){
            if (EmailValidator.getInstance(true, true).isValid(result)){
                return result;
            }else {
                return "";
            }
        }
        if (rowNameLowerCase.contains("phone")){
            final String onlyNumber = result.replaceAll("[^\\d.]", "").replace('.', ' ').replace(" ", "");
            if (onlyNumber.length() < 5){
                return "";
            }else{
                return onlyNumber;
            }
        }
        return result;
    }

    public final UploadDescriptor ingest(final File file,
                                         final String uploadName,
                                         final List<UploadMapping> mappings){
        if (getUploadDescriptorByName(uploadName) != null){
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Upload with name '" + uploadName + "' already exists");
        }
        final CsvUtil.CsvIterator iterator = CsvUtil.parseCsv(file);
        if (getUploadDescriptorByName(uploadName) != null){
            try{iterator.close();}catch (final Throwable ignored){}
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Upload with name '" + uploadName + "' already exists");
        }
        ensureMappingIndexes(mappings);
        final UploadDescriptor uploadDescriptor = repository.createUpload(uploadName, mappings, iterator);
        sourceFilesRepository.saveSourceFile(uploadDescriptor.name, file);
        Thread.startVirtualThread(()->{
            try {
                while (iterator.hasNext()){
                    final CsvUtil.CsvRecord record = iterator.next();
                    final List<Tuple2<String, String>> records = new ArrayList<>(mappings.size());
                    for(final UploadMapping mapping : mappings){
                        final StringBuilder sb = new StringBuilder(128);
                        //Iterate over source columns as more than one is allowed (e.g. first and last name)
                        for (int i = 0; i < mapping.sourceColumns.size() - 1; i++) {
                            sb.append(extractAndValidate(record, mapping.sourceColumns.get(i), mapping.transformations)).append(" ");
                        }
                        sb.append(extractAndValidate(record, mapping.sourceColumns.getLast(), mapping.transformations));

                        final String resultValue = StringTransformer.transform(sb.toString(), mapping.transformations);
                        if (!StringUtil.isNullOrBlank(resultValue)){
                            records.add(new Tuple2<>(mapping.destinationColumn, resultValue));
                        }
                    }
                    repository.addRecord(uploadDescriptor, iterator, records);
                }
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

    public final List<String> listAllColumns(){
        return columnsRepository.listAllColumn();
    }
}
