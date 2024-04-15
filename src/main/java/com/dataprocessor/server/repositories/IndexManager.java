package com.dataprocessor.server.repositories;

import jakarta.annotation.PostConstruct;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public final class IndexManager {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Neo4jManager neo4jManager;
    @Autowired
    public IndexManager(final Neo4jManager neo4jManager){
        this.neo4jManager = neo4jManager;
    }

    @PostConstruct
    private final void ensureIndexes(){
        ensureIndex("iUploadsNameIndex", "Upload", "name");
        ensureIndex("iUploadsTimeIndex", "Upload", "timeStamp");
    }

    public final void ensureIndex(final String indexName, final String nodeType, final String field){
        final String query = "CREATE INDEX " + indexName + " IF NOT EXISTS FOR (n:" + nodeType + ") ON (n." + field + ")";
        logger.info("Ensuring index: '{}'.", query);
        try (final var session = neo4jManager.getDriver().session(SessionConfig.builder().withDatabase(neo4jManager.getDatabase()).build())) {
            session.executeWrite(tx-> tx.run(query).consume());
        }catch (final Throwable cause){
            logger.error("Index validation failed. Query: {}. Cause:", query, cause);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to ensure index", cause);
        }
    }
}
