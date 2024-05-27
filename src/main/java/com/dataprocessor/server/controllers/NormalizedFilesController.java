package com.dataprocessor.server.controllers;

import com.dataprocessor.server.services.NormalizedFilesService;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

@RestController
public class NormalizedFilesController {

    private final NormalizedFilesService service;

    @Autowired
    public NormalizedFilesController(final NormalizedFilesService service){
        this.service = service;
    }

    private static final class FileInputStreamWrapper extends FileInputStream{
        private final File file;
        public FileInputStreamWrapper(@NotNull final File file) throws FileNotFoundException {
            super(file);
            this.file = file;
        }

        @Override
        public final void close() throws IOException {
            try{
                super.close();
            }catch (final IOException io){
                throw io;
            }finally {
                file.delete();
            }
        }
    }

    @GetMapping(value = "/normalizedUploads", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public final ResponseEntity<InputStreamResource> getNormalized(@RequestParam(value = "name")final String name) throws FileNotFoundException {
        final File file = service.getNormalizedFile(name);
        final FileInputStreamWrapper inputStream = new FileInputStreamWrapper(file);
        final InputStreamResource resource = new InputStreamResource(inputStream);
        final HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set(HttpHeaders.LAST_MODIFIED, String.valueOf(file.lastModified()));
        httpHeaders.set(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + name + "\"");
        httpHeaders.set(HttpHeaders.CONTENT_LENGTH, String.valueOf(file.length()));
        return ResponseEntity.ok()
                .headers(httpHeaders)
                .contentLength(file.length())
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(resource);
    }
}
