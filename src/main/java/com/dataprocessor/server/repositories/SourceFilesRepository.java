package com.dataprocessor.server.repositories;

import com.dataprocessor.server.utils.TempFileUtil;
import io.minio.*;
import io.minio.errors.*;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.*;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

@Service
public final class SourceFilesRepository {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final MinioClient minioClient;
    private final String bucket;
    @Autowired
    public SourceFilesRepository(@Value("${minio.endpoint}") final String uri,
                                 @Value("${minio.user}") final String userName,
                                 @Value("${minio.password}") final String password,
                                 @Value("${minio.buckets.uploads}") final String bucket) throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
        this.bucket = bucket;
        this.minioClient = MinioClient.builder()
                .endpoint(uri)
                .credentials(userName, password)
                .build();
        if (!this.minioClient.bucketExists(BucketExistsArgs.builder().bucket(this.bucket).build())){
            this.minioClient.makeBucket(MakeBucketArgs.builder().bucket(this.bucket).build());
        }
    }
    public final void saveSourceFile(final String name, final File file){
        try {
            minioClient.putObject(PutObjectArgs
                    .builder()
                    .bucket(bucket)
                    .object(name)
                    .stream(new FileInputStream(file), file.length(), -1)
                    .build());
        }catch (final Throwable cause){
            logger.warn("Failed to save source file '{}'", name, cause);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to upload source file '" + name + "'.", cause);
        }
    }

    public final File getSourceFile(final String name){
        try {
            final File res = TempFileUtil.createTmpFile(".data");
            FileUtils.copyInputStreamToFile(minioClient.getObject(GetObjectArgs.builder()
                    .bucket(bucket)
                    .object(name)
                    .build()), res);
            return res;
        }catch (final Throwable cause){
            logger.warn("Failed to get source file '{}'", name, cause);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to get source file '" + name + "'.", cause);
        }
    }
}
