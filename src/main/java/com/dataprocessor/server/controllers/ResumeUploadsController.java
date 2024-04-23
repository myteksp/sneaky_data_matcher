package com.dataprocessor.server.controllers;

import com.dataprocessor.server.entities.UploadDescriptor;
import com.dataprocessor.server.services.UploadsService;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public final class ResumeUploadsController {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private final UploadsService service;

    @Autowired
    public ResumeUploadsController(final UploadsService service){
        this.service = service;
    }

    @PostConstruct
    private final void  init(){
        Thread.startVirtualThread(()->{
            logger.info("Started resuming uploads.");

            for(final UploadDescriptor descriptor : service.listUnfinishedUploads()){
                logger.info("Resuming: {}", descriptor);
                try {
                    final UploadDescriptor result = service.continueIngestion(descriptor.name);
                    if (result == null){
                        logger.warn("{} - NOT FOUND.", descriptor.name);
                    }else{
                        logger.info("'{}' - successfully resumed.", result.name);
                    }
                }catch (final Throwable cause){
                    logger.warn("Failed to resume '{}'. Cause: ", descriptor, cause);
                }
            }
        });
    }
}
