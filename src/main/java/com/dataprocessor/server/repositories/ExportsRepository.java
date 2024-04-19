package com.dataprocessor.server.repositories;

import com.dataprocessor.server.entities.SearchEntity;
import com.dataprocessor.server.utils.json.JSON;
import io.minio.*;
import io.minio.errors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

@Service
public final class ExportsRepository {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final MinioClient minioClient;
    private final String bucket;
    private final String searchesBucket;

    @Autowired
    public ExportsRepository(@Value("${minio.endpoint}") final String uri,
                             @Value("${minio.user}") final String userName,
                             @Value("${minio.password}") final String password,
                             @Value("${minio.buckets.exports}") final String bucket,
                             @Value("${minio.buckets.exportsSearches}") final String searchesBucket) throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
        this.bucket = bucket;
        this.searchesBucket = searchesBucket;
        this.minioClient = MinioClient.builder()
                .endpoint(uri)
                .credentials(userName, password)
                .build();
        if (!this.minioClient.bucketExists(BucketExistsArgs.builder().bucket(this.bucket).build())){
            this.minioClient.makeBucket(MakeBucketArgs.builder().bucket(this.bucket).build());
        }
        if (!this.minioClient.bucketExists(BucketExistsArgs.builder().bucket(this.searchesBucket).build())){
            this.minioClient.makeBucket(MakeBucketArgs.builder().bucket(this.searchesBucket).build());
        }
    }

    public final boolean doesFileExist(final String name){
        try(final InputStream ignored = minioClient.getObject(GetObjectArgs.builder()
                        .bucket(bucket)
                        .object(name)
                .build())){
            return true;
        }catch (final Throwable cause){
            return false;
        }
    }

    public final InputStream getFile(final String name){
        try {
            return minioClient.getObject(GetObjectArgs.builder()
                    .bucket(bucket)
                    .object(name)
                    .build());
        }catch (final Throwable cause){
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to get file from minio.", cause);
        }
    }

    public final SearchEntity getSearchEntity(final String name){
        try {
            final byte[] bytes = minioClient.getObject(GetObjectArgs.builder()
                    .bucket(bucket)
                    .object(name)
                    .build()).readAllBytes();
            return JSON.fromJson(new String(bytes, StandardCharsets.UTF_8), SearchEntity.class);
        }catch (final Throwable cause){
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to get file from minio.", cause);
        }
    }

    public final void saveFile(final String name, final File file){
        try {
            minioClient.putObject(PutObjectArgs
                    .builder()
                    .bucket(bucket)
                    .object(name)
                    .stream(new FileInputStream(file), file.length(), -1)
                    .build());
        }catch (final Throwable cause){
            logger.warn("Failed to save source file '{}'", name, cause);
        }finally {
            final boolean deleteOp = file.delete();
            if (!deleteOp){
                logger.warn("Failed to delete temp file: {}", file.getAbsolutePath());
            }
        }
    }

    public final void saveSearch(final String name, final SearchEntity search){
        final byte[] data = JSON.toJson(search).getBytes(StandardCharsets.UTF_8);
        try {
            minioClient.putObject(PutObjectArgs
                    .builder()
                    .bucket(searchesBucket)
                    .object(name)
                    .stream(new ByteArrayInputStream(data), data.length, -1)
                    .build());
        }catch (final Throwable cause){
            logger.warn("Failed to save source file '{}'", name, cause);
        }
    }
}
