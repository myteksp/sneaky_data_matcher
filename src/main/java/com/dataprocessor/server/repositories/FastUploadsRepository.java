package com.dataprocessor.server.repositories;

import com.dataprocessor.server.entities.LogicalPredicate;
import com.dataprocessor.server.entities.UploadDescriptor;
import com.dataprocessor.server.entities.UploadMapping;
import com.dataprocessor.server.services.RecordValidationUtilService;
import com.dataprocessor.server.utils.ListUtils;
import com.dataprocessor.server.utils.StringTransformer;
import com.dataprocessor.server.utils.StringUtil;
import com.dataprocessor.server.utils.TryUtil;
import com.dataprocessor.server.utils.csv.CsvUtil;
import com.dataprocessor.server.utils.tuples.Tuple2;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public final class FastUploadsRepository {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String url;
    private final String dbName;
    private final String userName;
    private final String password;
    private final String tableName;
    private final RecordValidationUtilService recordValidationUtilService;

    @Autowired
    public FastUploadsRepository(@Value("${mysql.url}") final String url,
                                 @Value("${mysql.dbName}") final String dbName,
                                 @Value("${mysql.user}") final String userName,
                                 @Value("${mysql.password}") final String password,
                                 @Value("${mysql.table}") final String tableName,
                                 final RecordValidationUtilService recordValidationUtilService){
        this.url = url;
        this.dbName = dbName;
        this.userName = userName;
        this.password = password;
        this.tableName = tableName;
        this.recordValidationUtilService = recordValidationUtilService;
    }

    public final List<Map<String, List<String>>> unlimitedByUploads(final List<SearchRepository.SearchQuery> queries,
                                                                     final LogicalPredicate predicate,
                                                                     final List<String> joinByColumns,
                                                                     final int maxJoinDepth,
                                                                     final int skip,
                                                                     final int limit){
        final StringBuilder queryBuilder = new StringBuilder("SELECT * FROM ").append(tableName).append(" WHERE ");

        for (int i = 0; i < queries.size(); i++) {
            final SearchRepository.SearchQuery condition = queries.get(i);
            if (condition.queryType == SearchRepository.SearchQuery.QueryType.MATCHES){
                queryBuilder.append(condition.node).append(" = ?");
            }else{
                queryBuilder.append(condition.node).append(" LIKE ?");
            }
            if (i < queries.size() - 1) {
                if (predicate == LogicalPredicate.AND){
                    queryBuilder.append(" AND ");
                }else{
                    queryBuilder.append(" OR ");
                }
            }
        }
        queryBuilder.append(" LIMIT ").append(limit).append(" OFFSET ").append(skip).append(";");
        final String query = queryBuilder.toString();
        logger.info("Executing unbound query: {}", query);
        final List<Map<String, List<String>>> result = new ArrayList<>(limit);
        TryUtil.attempt(this::createConnection).doTry(connection->{
            TryUtil.attempt(()->connection.prepareStatement(query)).doTry(preparedStatement->{
                TryUtil.attempt(()->{
                    for (int i = 0; i < queries.size(); i++) {
                        final SearchRepository.SearchQuery q = queries.get(i);
                        if (q.queryType == SearchRepository.SearchQuery.QueryType.MATCHES){
                            preparedStatement.setString(i + 1, q.query);
                        }else if (q.queryType == SearchRepository.SearchQuery.QueryType.STARTS_WITH){
                            preparedStatement.setString(i + 1, q.query + "%");
                        } else if (q.queryType == SearchRepository.SearchQuery.QueryType.ENDS_WITH){
                            preparedStatement.setString(i + 1, "%" + q.query);
                        }else{
                            preparedStatement.setString(i + 1, "%" + q.query + "%");
                        }
                    }
                    final ResultSet resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()) {
                        final ResultSetMetaData metaData = resultSet.getMetaData();
                        final int columnCount = metaData.getColumnCount();
                        for (int i = 1; i <= columnCount; i++) {
                            final String columnName = metaData.getColumnName(i);
                            final String columnValue = resultSet.getString(i);
                            result.add(Map.of(columnName, ListUtils.listOfString(columnValue)));
                        }
                    }
                }).doTry();
            });
        });
        return result;
    }

    public final List<Map<String, List<String>>> limitedByUploads(final List<SearchRepository.SearchQuery> queries,
                                                                   final LogicalPredicate predicate,
                                                                   final List<String> limitByUploads,
                                                                   final List<String> joinByColumns,
                                                                   final int maxJoinDepth,
                                                                   final int skip,
                                                                   final int limit){
        final StringBuilder queryBuilder = new StringBuilder("SELECT * FROM ").append(tableName).append(" WHERE (");

        for (int i = 0; i < limitByUploads.size(); i++) {
            queryBuilder.append("UploadName").append(" = '").append(limitByUploads.get(i)).append("'");
            if (i < queries.size() - 1) {
                queryBuilder.append(" OR ");
            }
        }
        queryBuilder.append(") AND (");

        for (int i = 0; i < queries.size(); i++) {
            final SearchRepository.SearchQuery condition = queries.get(i);
            if (condition.queryType == SearchRepository.SearchQuery.QueryType.MATCHES){
                queryBuilder.append(condition.node).append(" = ?");
            }else{
                queryBuilder.append(condition.node).append(" LIKE ?");
            }
            if (i < queries.size() - 1) {
                if (predicate == LogicalPredicate.AND){
                    queryBuilder.append(" AND ");
                }else{
                    queryBuilder.append(" OR ");
                }
            }
        }
        queryBuilder.append(") ");

        queryBuilder.append(" LIMIT ").append(limit).append(" OFFSET ").append(skip).append(";");
        final String query = queryBuilder.toString();
        logger.info("Executing unbound query: {}", query);
        final List<Map<String, List<String>>> result = new ArrayList<>(limit);
        TryUtil.attempt(this::createConnection).doTry(connection->{
            TryUtil.attempt(()->connection.prepareStatement(query)).doTry(preparedStatement->{
                TryUtil.attempt(()->{
                    for (int i = 0; i < queries.size(); i++) {
                        final SearchRepository.SearchQuery q = queries.get(i);
                        if (q.queryType == SearchRepository.SearchQuery.QueryType.MATCHES){
                            preparedStatement.setString(i + 1, q.query);
                        }else if (q.queryType == SearchRepository.SearchQuery.QueryType.STARTS_WITH){
                            preparedStatement.setString(i + 1, q.query + "%");
                        } else if (q.queryType == SearchRepository.SearchQuery.QueryType.ENDS_WITH){
                            preparedStatement.setString(i + 1, "%" + q.query);
                        }else{
                            preparedStatement.setString(i + 1, "%" + q.query + "%");
                        }
                    }
                    final ResultSet resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()) {
                        final ResultSetMetaData metaData = resultSet.getMetaData();
                        final int columnCount = metaData.getColumnCount();
                        for (int i = 1; i <= columnCount; i++) {
                            final String columnName = metaData.getColumnName(i);
                            final String columnValue = resultSet.getString(i);
                            result.add(Map.of(columnName, ListUtils.listOfString(columnValue)));
                        }
                    }
                }).doTry();
            });
        });
        return result;
    }

    public final void fastSave(final UploadDescriptor upload, final File file){
        logger.info("Uploading CSV to MYSQL...");
        final long csvUploadStartTime = System.currentTimeMillis();
        ensureTableStructure(upload);
        final CsvUtil.CsvIterator iterator = CsvUtil.parseCsv(file, false);
        TryUtil.attempt(this::createConnection)
                .onFinally(()-> {
                    TryUtil.attempt(iterator::close).doTry();
                    logger.info("CSV upload '{}' finished in {} milliseconds.", upload.name, (System.currentTimeMillis() - csvUploadStartTime));
                })
                .doTry(connection-> {
                    logger.info("Connection to MYSQL established. Uploading...");
                    while (iterator.hasNext()) {
                        final List<List<Tuple2<String, String>>> rows = iterator.getBulk(100).stream().map(r -> {
                            final List<Tuple2<String, String>> records = new ArrayList<>(upload.mappings.size());
                            for (final UploadMapping mapping : upload.mappings) {
                                final StringBuilder sb = new StringBuilder(128);
                                //Iterate over source columns as more than one is allowed (e.g. first and last name)
                                for (int i = 0; i < mapping.sourceColumns.size() - 1; i++) {
                                    sb.append(recordValidationUtilService.extractAndValidate(r, mapping.sourceColumns.get(i), mapping.transformations)).append(" ");
                                }
                                sb.append(recordValidationUtilService.extractAndValidate(r, mapping.sourceColumns.getLast(), mapping.transformations));

                                final String resultValue = StringTransformer.transform(sb.toString(), mapping.transformations);
                                if (StringUtil.isNullOrBlank(resultValue)) {
                                    records.add(new Tuple2<>(mapping.destinationColumn, ""));
                                } else {
                                    records.add(new Tuple2<>(mapping.destinationColumn, resultValue));
                                }
                            }
                            return records;
                        }).toList();

                        if (!rows.isEmpty()) {
                            upload.mappings.sort(Comparator.comparing(o -> o.destinationColumn));
                            final StringBuilder queryBuilder = new StringBuilder(1024 + rows.size() * 1024);
                            queryBuilder.append("INSERT INTO ").append(tableName).append(" (");
                            for (int i = 0; i < upload.mappings.size(); i++) {
                                queryBuilder.append(upload.mappings.get(i).destinationColumn).append(", ");
                            }
                            queryBuilder.append("UploadName) \n");
                            queryBuilder.append("VALUES \n");
                            final int lastIndex = rows.size() - 1;
                            for (int i = 0; i < lastIndex; i++) {
                                rows.get(i).sort(Comparator.comparing(o -> o.v1));
                                addRowToQueryBuilder(queryBuilder, rows.get(i), upload);
                                queryBuilder.append(", \n");
                            }
                            rows.get(lastIndex).sort(Comparator.comparing(o -> o.v1));
                            addRowToQueryBuilder(queryBuilder, rows.get(lastIndex), upload);
                            queryBuilder.append(";");
                            final String query = queryBuilder.toString();
                            //logger.info("Inserting bulk: {}", query);
                            TryUtil.attempt(() -> connection.createStatement()).doTry(statement -> {
                                TryUtil.attempt(() -> {
                                    statement.executeUpdate(query);
                                }).onError(error -> logger.error("Failed to execute bulk insert: ", error)).doTry();
                            });
                        }
                    }
                });
    }

    private final void addRowToQueryBuilder(final StringBuilder sb, final List<Tuple2<String, String>> row, final UploadDescriptor uploadDescriptor){
        sb.append("(");
        for (int i = 0; i < row.size(); i++) {
            sb.append("'").append(escapeSqlString(row.get(i).v2)).append("', ");
        }
        sb.append("'").append(uploadDescriptor.name).append("')");
    }

    private static final String escapeSqlString(String value) {
        final StringBuilder escapedValue = new StringBuilder(value.length() * 2);
        for (final char c : value.toCharArray()) {
            switch (c) {
                case '\0':
                    escapedValue.append("\\0");
                    break;
                case '\n':
                    escapedValue.append("\\n");
                    break;
                case '\r':
                    escapedValue.append("\\r");
                    break;
                case '\\':
                    escapedValue.append("\\\\");
                    break;
                case '\'':
                    escapedValue.append("\\'");
                    break;
                case '\"':
                    escapedValue.append("\\\"");
                    break;
                case '\b':
                    escapedValue.append("\\b");
                    break;
                case '\t':
                    escapedValue.append("\\t");
                    break;
                case '\u001A':
                    escapedValue.append("\\Z");
                    break;
                case '%':
                    escapedValue.append("\\%");
                    break;
                case '_':
                    escapedValue.append("\\_");
                    break;
                default:
                    escapedValue.append(c);
                    break;
            }
        }
        return escapedValue.toString();
    }

    public final void removeSave(final UploadDescriptor upload){
        TryUtil.attempt(this::createConnection).doTry(connection->{
            TryUtil.attempt(()->connection.createStatement()).doTry(statement->{
                TryUtil.attempt(()->{
                    final String query = String.format("DELETE FROM %s WHERE UploadName='%s';", tableName, upload.name);
                    logger.info("Removing upload '{}'. Query: {}", upload.name, query);
                    statement.executeUpdate(query);
                }).doTry();
            });
        });
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
