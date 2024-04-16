package com.dataprocessor.server.repositories;

import com.dataprocessor.server.entities.UploadDescriptor;
import com.dataprocessor.server.entities.UploadMapping;
import com.dataprocessor.server.utils.StringUtil;
import com.dataprocessor.server.utils.csv.CsvUtil;
import com.dataprocessor.server.utils.json.JSON;
import com.dataprocessor.server.utils.tuples.Tuple2;
import org.neo4j.driver.Record;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Service
public class UploadRepository {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Neo4jManager neo4jManager;
    @Autowired
    public UploadRepository(final Neo4jManager neo4jManager){
        this.neo4jManager = neo4jManager;
    }

    public final void addRecord(final UploadDescriptor upload, final CsvUtil.CsvIterator iterator, final List<Tuple2<String, String>> record){
        if (record.isEmpty())
            return;
        final Map<String, Object> queryParams = new HashMap<>(16 + (record.size() * 2));
        queryParams.put("uploadName", upload.name);
        queryParams.put("uploadProcessed", iterator.getCurrentRow());
        final StringBuilder query = new StringBuilder(1024);
        query.append("MERGE (upload:Upload {name:$uploadName}) SET upload.processed=$uploadProcessed").append('\n');
        queryParams.put("rowId", StringUtil.generateId());
        query.append("MERGE (upload)-[:OWNS]->(row:Row {rowId:$rowId})").append('\n');
        for (int i = 0; i < record.size(); i++) {
            final Tuple2<String, String> recordValue = record.get(i);
            final String paramName = "p" + i;
            query.append("MERGE (n").append(i).append(":").append(recordValue.v1).append(" {value:$").append(paramName).append("})").append('\n');
            queryParams.put(paramName, recordValue.v2);
        }
        for (int i = 0; i < record.size(); i++) {
            query.append("MERGE (n").append(i).append(")<-[:OWNS]-(row)").append('\n');
        }
        final String queryString = query.toString();
        try (final var session = neo4jManager.getDriver().session(SessionConfig.builder().withDatabase(neo4jManager.getDatabase()).build())) {
            session.executeWrite(tx-> tx.run(queryString, queryParams).consume());
        }catch (final Throwable cause){
            logger.error("Failed to add a record. Query: '{}'", queryString, cause);
        }
        logger.info("Upload '{}' Row {} out of {}.", upload.name, iterator.getCurrentRow(), iterator.getTotalRows());
    }
    public final UploadDescriptor createUpload(final String uploadName,
                                               final List<UploadMapping> mappings,
                                               final CsvUtil.CsvIterator iterator){
        final String query = "CREATE (u:Upload {name: $name, timeStamp: $timeStamp, mappings: $mappings, status: $status, processed: $processed, outOf: $outOf});";
        try (final var session = neo4jManager.getDriver().session(SessionConfig.builder().withDatabase(neo4jManager.getDatabase()).build())) {
            session.executeWrite(tx-> tx.run(query, Map.of("name", uploadName,
                    "timeStamp", iterator.timeStamp(),
                    "mappings", JSON.toJson(mappings),
                    "status", UploadDescriptor.Status.PROCESSING.toString(),
                    "processed", iterator.getCurrentRow(),
                    "outOf", iterator.getTotalRows())).consume());
        }catch (final Throwable cause){
            logger.error("Failed to create upload descriptor.", cause);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to create upload descriptor.", cause);
        }
        return getUploadByName(uploadName);
    }
    public final void completeUploadWithError(final UploadDescriptor upload){
        final String query = "MATCH (u:Upload {name:$name}) SET u.status=$status;";
        try (final var session = neo4jManager.getDriver().session(SessionConfig.builder().withDatabase(neo4jManager.getDatabase()).build())) {
            session.executeWrite(tx-> tx.run(query, Map.of("name", upload.name, "status", UploadDescriptor.Status.FINISHED_WITH_ERROR.toString())).consume());
        }catch (final Throwable cause){
            logger.error("Failed to complete upload with error. Query: {}. Cause:", query, cause);
        }
    }
    public final void completeUploadWithSuccess(final UploadDescriptor upload){
        final String query = "MATCH (u:Upload {name:$name}) SET u.status=$status;";
        try (final var session = neo4jManager.getDriver().session(SessionConfig.builder().withDatabase(neo4jManager.getDatabase()).build())) {
            session.executeWrite(tx-> tx.run(query, Map.of("name", upload.name, "status", UploadDescriptor.Status.FINISHED.toString())).consume());
        }catch (final Throwable cause){
            logger.error("Failed to complete upload. Query: {}. Cause:", query, cause);
        }
    }

    public final List<UploadDescriptor> listUnfinishedUploads(){
        final String query = "MATCH (n:Upload) WHERE n.status = $status RETURN n ORDER BY n.timeStamp;";
        try (final var session = neo4jManager.getDriver().session(SessionConfig.builder().withDatabase(neo4jManager.getDatabase()).build())) {
            return session.run(query, Map.of("status", UploadDescriptor.Status.PROCESSING.toString())).list()
                    .stream()
                    .map(r->valueToDescriptor(r.get("n")))
                    .collect(Collectors.toList());
        }catch (final Throwable cause){
            logger.error("Failed to list unfinished uploads.", cause);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to list unfinished uploads.", cause);
        }
    }

    public final List<UploadDescriptor> listFinishedUploads(final int skip, final int limit){
        final String query = "MATCH (n:Upload) WHERE NOT n.status=$status RETURN n ORDER BY n.timeStamp SKIP " + skip + " LIMIT " + limit+ ";";
        try (final var session = neo4jManager.getDriver().session(SessionConfig.builder().withDatabase(neo4jManager.getDatabase()).build())) {
            return session.run(query, Map.of("status", UploadDescriptor.Status.PROCESSING.toString())).list()
                    .stream()
                    .map(r->valueToDescriptor(r.get("n")))
                    .collect(Collectors.toList());
        }catch (final Throwable cause){
            logger.error("Failed to list unfinished uploads.", cause);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to list unfinished uploads.", cause);
        }
    }

    public final UploadDescriptor getUploadByName(final String uploadName){
        final String query = "MATCH (n:Upload) WHERE n.name = $name RETURN n;";
        final Record record;
        try (final var session = neo4jManager.getDriver().session(SessionConfig.builder().withDatabase(neo4jManager.getDatabase()).build())) {
            final List<Record> lst = session.run(query, Map.of("name", uploadName)).list();
            if (lst.isEmpty())
                return null;
            record = lst.getFirst();
        }catch (final Throwable cause){
            logger.error("Failed to find upload with name '{}'. Query: {}. Cause:", uploadName, query, cause);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to find uploads", cause);
        }
        return valueToDescriptor(record.get("n"));
    }

    private static final UploadDescriptor valueToDescriptor(final org.neo4j.driver.Value v){
        return new UploadDescriptor(
                v.get("name").asString(),
                v.get("processed").asLong(),
                v.get("outOf").asLong(),
                v.get("timeStamp").asLong(),
                JSON.fromJson(v.get("mappings").asString(), UploadMapping.UploadMappingList.class),
                UploadDescriptor.Status.valueOf(v.get("status").asString()));
    }
}
