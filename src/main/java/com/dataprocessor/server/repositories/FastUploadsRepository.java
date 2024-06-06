package com.dataprocessor.server.repositories;

import com.dataprocessor.server.entities.UploadDescriptor;
import com.dataprocessor.server.utils.TryUtil;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public final class FastUploadsRepository {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String url;
    private final String dbName;
    private final String userName;
    private final String password;
    private final String tableName;

    @Autowired
    public FastUploadsRepository(@Value("${mysql.url}") final String url,
                                 @Value("${mysql.dbName}") final String dbName,
                                 @Value("${mysql.user}") final String userName,
                                 @Value("${mysql.password}") final String password,
                                 @Value("${mysql.table}") final String tableName){
        this.url = url;
        this.dbName = dbName;
        this.userName = userName;
        this.password = password;
        this.tableName = tableName;
    }


    public final void fastSave(final UploadDescriptor upload, final File file){
        ensureTableStructure(upload);
    }

    public final void removeSave(final UploadDescriptor upload){

    }

    private final void ensureTableStructure(final UploadDescriptor upload){
        final List<String> columnNames = new ArrayList<>(upload.mappings.size() + 5);
        columnNames.add("UploadName");
        columnNames.addAll(upload.mappings.stream().map(m->m.destinationColumn).toList());

        if (doesTableExist()){
            for(final String columnName : getExistingColumns()){
                columnNames.remove(columnName);
            }
            if (!columnNames.isEmpty()){
                addColumnsToTable(columnNames);
            }
        }else{
            createTable(columnNames);
        }
    }

    private final void addColumnsToTable(final List<String> columnNames){
        final StringBuilder alterTableSQL = new StringBuilder("ALTER TABLE ");
        alterTableSQL.append(tableName);
        for (int i = 0; i < columnNames.size(); i++) {
            alterTableSQL.append(" ADD COLUMN ").append(columnNames.get(i)).append(" TEXT");
            if (i < columnNames.size() - 1) {
                alterTableSQL.append(", ");
            }
        }
        alterTableSQL.append(";");
        final String query = alterTableSQL.toString();
        logger.info("Altering table: {}", query);
        TryUtil.attempt(this::createConnection).doTry(connection->{
            TryUtil.attempt(()->connection.createStatement()).doTry(statement->{
                TryUtil.attempt(()->{
                    statement.executeUpdate(query);
                }).doTry();
            });
        });
    }

    private final List<String> getExistingColumns(){
        final List<String> result = new ArrayList<>(30);
        TryUtil.attempt(this::createConnection).doTry(connection->{
            TryUtil.attempt(()-> connection.prepareStatement("""
                    SELECT COLUMN_NAME \s
                    FROM information_schema.columns \s
                    WHERE table_schema = ? \s
                      AND table_name = ? ;
                    """)).doTry(statement->{
                        TryUtil.attempt(()->{
                            statement.setString(1, dbName);
                            statement.setString(2, tableName);
                            TryUtil.attempt(()->statement.executeQuery()).doTry(rs->{
                                TryUtil.attempt(()->{
                                    while (rs.next()) {
                                        result.add(rs.getString("COLUMN_NAME"));
                                    }
                                }).doTry();
                            });
                        }).doTry();
                    });
        });
        return result;
    }

    private final void createTable(final List<String> columnNames){
        final StringBuilder createTableSQL = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        createTableSQL.append(tableName).append(" (");
        for (int i = 0; i < columnNames.size(); i++) {
            createTableSQL.append(columnNames.get(i)).append(" TEXT");
            if (i < columnNames.size() - 1) {
                createTableSQL.append(", ");
            }
        }
        createTableSQL.append(");");
        final String query = createTableSQL.toString();
        logger.info("Creating the table: {}", query);
        TryUtil.attempt(this::createConnection).doTry(connection->{
            TryUtil.attempt(()-> connection.createStatement()).doTry(statement->{
                TryUtil.attempt(()-> statement.executeUpdate(query))
                        .onError(error-> logger.error("Failed to create table: ", error))
                        .doTry();
            });
        });
    }

    private final boolean doesTableExist(){
        final AtomicBoolean tableExists = new AtomicBoolean(false);
        TryUtil.attempt(this::createConnection).doTry(connection->{
            TryUtil.attempt(()-> connection.prepareStatement("""
                    SELECT COUNT(*) AS table_exists
                    FROM information_schema.tables\s
                    WHERE table_schema = ? \s
                      AND table_name = ? ;
                    """)).doTry(statement->{
                TryUtil.attempt(()->{
                    statement.setString(1, dbName);
                    statement.setString(2, tableName);
                    final ResultSet rs = statement.executeQuery();
                    if (rs.next()) {
                        tableExists.set(rs.getInt("table_exists") > 0);
                    }
                }).doTry();
            });
        });
        return tableExists.get();
    }

    @PostConstruct
    private final void init() throws SQLException {
        ensureDatabaseExists();
        TryUtil.attempt(this::createConnection).doTry(connection->{
            TryUtil.attempt(()-> connection.createStatement()).doTry(statement->{
                TryUtil.attempt(()-> statement.executeQuery("SELECT VERSION()")).doTry(resultSet->{
                    TryUtil.attempt(()->{
                        while (resultSet.next()) {
                            logger.info("MariaDB Version: {}", resultSet.getString(1));
                        }
                    }).doTry();
                });
            });
        });
        logger.info("Database '{}' connected. Table exists: {}", dbName, doesTableExist());
    }

    private final void ensureDatabaseExists() throws SQLException {
        try(final Connection con = DriverManager.getConnection(url, userName, password)){
            final Statement statement = con.createStatement();
            statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName);
            statement.close();
        }
        logger.info("Database '{}' ensured.", dbName);
    }

    private final Connection createConnection() throws SQLException {
        if (url.endsWith("/")) {
            return DriverManager.getConnection(url + dbName, userName, password);
        }else{
            return DriverManager.getConnection(url + "/" + dbName, userName, password);
        }
    }
}
