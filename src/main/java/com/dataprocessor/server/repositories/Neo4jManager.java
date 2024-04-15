package com.dataprocessor.server.repositories;

import jakarta.annotation.PreDestroy;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class Neo4jManager {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Driver driver;
    private final String database;
    @Autowired
    public Neo4jManager(@Value("${neo4j.uri}") final String uri,
                        @Value("${neo4j.user}") final String userName,
                        @Value("${neo4j.password}") final String password,
                        @Value("${neo4j.db}") final String database){
        this.database = database;
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(userName, password));
    }

    public final Driver getDriver(){
        return driver;
    }

    public final String getDatabase(){
        return database;
    }

    @PreDestroy
    private final void close(){
        try {
            driver.close();
        }catch (final Throwable cause){
            logger.error("Failed to close neo4j driver.", cause);
        }
    }
}
