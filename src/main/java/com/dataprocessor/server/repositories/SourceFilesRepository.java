package com.dataprocessor.server.repositories;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.errors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
}
