package com.dataprocessor.server.repositories;

import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@Service
public final class ColumnsRepository {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Neo4jManager neo4jManager;

    public ColumnsRepository(final Neo4jManager neo4jManager){
        this.neo4jManager = neo4jManager;
    }

    public final List<String> listAllColumn(){
        try (final var session = neo4jManager.getDriver().session(SessionConfig.builder().withDatabase(neo4jManager.getDatabase()).build())) {
            return session.run("call db.labels();").list().stream()
                    .map(r->r.get(0).asString())
                    .filter(s -> Character.isLowerCase(s.charAt(0)))
                    .sorted(String::compareTo)
                    .toList();
        }catch (final Throwable cause){
            logger.error("Failed to list columns.", cause);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to list columns.", cause);
        }
    }
}
