package com.dataprocessor.server.controllers;

import com.dataprocessor.server.entities.UploadDescriptor;
import com.dataprocessor.server.services.UploadsService;
import com.dataprocessor.server.utils.TempFileUtil;
import com.dataprocessor.server.utils.UploadMappingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@RestController
@RequestMapping("/uploads")
public class UploadsController {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final UploadsService service;
    @Autowired
    public UploadsController(final UploadsService service){
        this.service = service;
    }

    @PostMapping(value = "/upload", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public final UploadDescriptor upload(@RequestParam("file") final MultipartFile file,
                                         @RequestParam("name") final String uploadName,
                                         @RequestParam("mappings") final List<String> mappings){
        return service.ingest(TempFileUtil.copyToTmpFile(file), uploadName, UploadMappingUtil.parse(mappings));
    }

    @PostMapping(value = "/nativeUpload", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public final UploadDescriptor nativeUpload(@RequestParam("file") final MultipartFile file,
                                               @RequestParam("name") final String uploadName,
                                               @RequestParam("mappings") final List<String> mappings){
        return service.nativeIngest(TempFileUtil.copyToTmpFile(file), uploadName, UploadMappingUtil.parse(mappings));
    }

    @GetMapping(value = "/listUnfinishedUploadDescriptors", produces = MediaType.APPLICATION_JSON_VALUE)
    public final List<UploadDescriptor> listUnfinishedUploads(){
        return service.listUnfinishedUploads();
    }

    @GetMapping(value = "/listFinishedUploadDescriptors", produces = MediaType.APPLICATION_JSON_VALUE)
    public final List<UploadDescriptor> listFinishedUploads(@RequestParam(value = "skip", defaultValue = "0") final int skip,
                                                            @RequestParam(value = "limit", defaultValue = "100") final int limit){
        return service.listFinishedUploads(skip, limit);
    }
    @GetMapping(value = "/getUploadDescriptor", produces = MediaType.APPLICATION_JSON_VALUE)
    public final UploadDescriptor getUpload(@RequestParam(value = "name") final String name){
        return service.getUploadDescriptorByName(name);
    }
}
