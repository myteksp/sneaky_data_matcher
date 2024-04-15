package com.dataprocessor.server.utils;

import org.springframework.http.HttpStatus;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.File;

public final class TempFileUtil {
    public static final File copyToTmpFile(final MultipartFile file){
        final File tmp = createTmpFile(".csv");
        try {
            file.transferTo(tmp);
        }catch (final Throwable cause){
            final boolean deleteRes = tmp.delete();
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to copy upload to tmp file. Tmp file deletion: " + deleteRes, cause);
        }
        return tmp;
    }
    public static final File createTmpFile(final String ext){
        try {
            final File file = File.createTempFile("tmp", ext);
            file.deleteOnExit();
            return file;
        }catch (final Throwable cause){
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to create temp file.", cause);
        }
    }
}
