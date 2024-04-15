package com.dataprocessor.server.repositories;

import com.dataprocessor.server.entities.LogicalPredicate;
import com.dataprocessor.server.utils.ListUtils;
import com.dataprocessor.server.utils.StringTransformer;
import com.dataprocessor.server.utils.StringUtil;
import com.dataprocessor.server.utils.json.JSON;
import com.dataprocessor.server.utils.tuples.Tuple2;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class SearchRepository {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Neo4jManager neo4jManager;
    @Autowired
    public SearchRepository(final Neo4jManager neo4jManager){
        this.neo4jManager = neo4jManager;
    }

    public final List<Map<String, List<String>>> search(final List<String> columnSearches,
                                                        final LogicalPredicate predicate,
                                                        final List<String> limitByUploads,
                                                        final List<String> joinByColumns,
                                                        final int maxJoinDepth,
                                                        final int skip,
                                                        final int limit){
        final List<SearchQuery> queries = columnSearches.stream().map(SearchQuery::new).toList();
        if (limitByUploads.isEmpty()){
            return unlimitedByUploads(queries, predicate, joinByColumns, maxJoinDepth, skip, limit);
        } else {
            return limitedByUploads(queries, predicate, limitByUploads, joinByColumns, maxJoinDepth, skip, limit);
        }
    }

    private final String buildWhereClause(final List<SearchQuery> queries,
                                          final LogicalPredicate predicate,
                                          final Map<String, Object> queryParams){
        final StringBuilder sb = new StringBuilder(1024);
        for (int i = 0; i < queries.size()-1; i++) {
            final String paramName = "p" + i;
            final SearchQuery q = queries.get(i);
            queryParams.put(paramName, q.query);
            switch (q.queryType){
                case MATCHES -> sb.append("n.value = $").append(paramName);
                case ENDS_WITH -> sb.append("n.value ends with $").append(paramName);
                case STARTS_WITH -> sb.append("n.value starts with $").append(paramName);
                case CONTAINS -> sb.append("n.value contains $").append(paramName);
            }
            sb.append(" ").append(predicate.toString()).append(" ");
        }
        final String paramName = "p" + queries.size();
        final SearchQuery q = queries.getLast();
        queryParams.put(paramName, q.query);
        switch (q.queryType){
            case MATCHES -> sb.append("n.value = $").append(paramName).append(" ");
            case ENDS_WITH -> sb.append("n.value ends with $").append(paramName).append(" ");
            case STARTS_WITH -> sb.append("n.value starts with $").append(paramName).append(" ");
            case CONTAINS -> sb.append("n.value contains $").append(paramName).append(" ");
        }
        return sb.toString();
    }

    private final String buildWhereClauseForUploads(final List<String> uploads,
                                                    final Map<String, Object> queryParams){
        final StringBuilder sb = new StringBuilder(1024);
        for (int i = 0; i < uploads.size() - 1; i++) {
            final String paramName = "u" + i;
            final String uploadName = uploads.get(i);
            queryParams.put(paramName, uploadName);
            sb.append("u.name = $").append(paramName).append(" OR ");
        }
        final String paramName = "u" + uploads.size();
        final String uploadName = uploads.getLast();
        queryParams.put(paramName, uploadName);
        sb.append("u.name = $").append(paramName);
        return sb.toString();
    }

    private final List<Map<String, List<String>>> unlimitedByUploads(final List<SearchQuery> queries,
                                                                     final LogicalPredicate predicate,
                                                                     final List<String> joinByColumns,
                                                                     final int maxJoinDepth,
                                                                     final int skip,
                                                                     final int limit){
        final Map<String, Object> queryParams = new HashMap<>(32);
        final String query = String.format("MATCH (n:%s) WHERE %s RETURN n SKIP %d LIMIT %d;",
                queries.stream().map(q->q.node).collect(Collectors.joining("|")),
                buildWhereClause(queries, predicate, queryParams),
                skip, limit);
        return enrichSeedSearch(query, queryParams, joinByColumns, maxJoinDepth);
    }

    private final List<Map<String, List<String>>> limitedByUploads(final List<SearchQuery> queries,
                                                                   final LogicalPredicate predicate,
                                                                   final List<String> limitByUploads,
                                                                   final List<String> joinByColumns,
                                                                   final int maxJoinDepth,
                                                                   final int skip,
                                                                   final int limit){
        final Map<String, Object> queryParams = new HashMap<>(32);
        final String query = String.format("MATCH (n:%s)<-[:OWNS]-(:Row)<-[:OWNS]-(u:Upload) WHERE (%s) AND (%s) RETURN n SKIP %d LIMIT %d;",
                queries.stream().map(q->q.node).collect(Collectors.joining("|")),
                buildWhereClause(queries, predicate, queryParams),
                buildWhereClauseForUploads(limitByUploads, queryParams),
                skip, limit);
        return enrichSeedSearch(query, queryParams, joinByColumns, maxJoinDepth);
    }

    private final List<Map<String, List<String>>> enrichSeedSearch(final String seedQuery,
                                                                   final Map<String, Object> queryParams,
                                                                   final List<String> joinByColumns,
                                                                   final int maxJoinDepth){
        final List<Map<String, List<String>>> result;
        try (final var session = neo4jManager.getDriver().session(SessionConfig.builder().withDatabase(neo4jManager.getDatabase()).build())) {
            result = session.run(seedQuery, queryParams).list().stream()
                    .map(r->{
                        final String seedNodeId = r.get("n").asNode().elementId();
                        final String query = String.format("""
                                MATCH (n)<-[:OWNS]-(src:Row) WHERE elementId(n) = '%s'
                                MATCH (src)-[:OWNS]->(r)
                                RETURN r;
                                """, seedNodeId);
                        final List<Tuple2<String, String>> nodes = session.run(query).stream().map(record -> {
                            final Node node = record.get("r").asNode();
                            return new Tuple2<>(ListUtils.getFirst(node.labels(), ""), node.get("value").asString());
                        }).toList();
                        final Map<String, List<String>> res = new HashMap<>(nodes.size() * 2);
                        res.put("_id", ListUtils.listOfString(seedNodeId));
                        for(final Tuple2<String, String> nd : nodes){
                            res.put(nd.v1, ListUtils.listOfString(nd.v2));
                        }
                        return res;
                    }).toList();
        }catch (final Throwable cause){
            logger.error("Failed to execute seed query: '{}'. Params: {}. Cause:", seedQuery, JSON.toJson(queryParams), cause);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to execute seed query.", cause);
        }
        if (joinByColumns.isEmpty() || maxJoinDepth <=0){
            return result;
        }else{
            return enrichSeedSearchWithJoins(result, joinByColumns, maxJoinDepth);
        }
    }

    private final List<Map<String, List<String>>> enrichSeedSearchWithJoins(List<Map<String, List<String>>> seedResults,
                                                                            final List<String> joinByColumns,
                                                                            final int maxJoinDepth){
        try (final var session = neo4jManager.getDriver().session(SessionConfig.builder().withDatabase(neo4jManager.getDatabase()).build())) {
            return seedResults.stream().map(seed->{
                final String sourceNodeId = seed.get("_id").getFirst();
                for(final String joinOn : joinByColumns){
                    if (seed.containsKey(joinOn)){
                        final String query = String.format("""
                                MATCH (n)<-[:OWNS]-(src:Row) WHERE elementId(n) = '%s'
                                MATCH (src)-[:OWNS]->(:%s)<-[:OWNS]-(src1)
                                MATCH (src1)-[:OWNS]->(r)
                                return r, src1;
                                """, sourceNodeId, joinOn);
                        final List<Tuple2<String, String>> recordLst = new ArrayList<>(maxJoinDepth);
                        try {
                            final Result queryResult = session.run(query);
                            final HashSet<String> idCounter = new HashSet<>(maxJoinDepth * 2);
                            while (queryResult.hasNext()){
                                final Record record = queryResult.next();
                                final String srcId = record.get("src1").asNode().elementId();
                                final Node node = record.get("r").asNode();
                                idCounter.add(srcId);
                                if (idCounter.size() > maxJoinDepth)
                                    break;

                                recordLst.add(new Tuple2<>(ListUtils.getFirst(node.labels(), ""), node.get("value").asString()));
                            }
                            for(final Tuple2<String, String> r : recordLst){
                                final List<String> rLst = seed.get(r.v1);
                                if (rLst == null){
                                    seed.put(r.v1, ListUtils.listOfString(r.v2));
                                }else{
                                    if (!rLst.contains(r.v2)){
                                        rLst.add(r.v2);
                                    }
                                }
                            }
                        }catch (final Throwable innerCause){
                            logger.error("Failed to execute join query. Cause:", innerCause);
                        }
                    }
                }
                return seed;
            }).toList();
        }catch (final Throwable cause){
            logger.error("Failed to execute enrichment query. Cause:", cause);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to execute enrichment query.", cause);
        }
    }




    private static final class SearchQuery{
        private final String node;
        private final String query;
        private final QueryType queryType;

        private SearchQuery(final String query){
            final String[] split = query.split(":");
            if (split.length < 2){
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Search query must be in form: <column:query>");
            }
            this.node = StringTransformer.transform(split[0], List.of(StringTransformer.Transformation.TLC, StringTransformer.Transformation.TRIM));
            if (StringUtil.isNullOrBlank(this.node)){
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Column name must not be empty.");
            }
            final String queryPart = StringTransformer.transform(split[1], List.of(StringTransformer.Transformation.TLC, StringTransformer.Transformation.TRIM));
            if (StringUtil.isNullOrBlank(queryPart)){
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Query must not be empty.");
            }
            if (queryPart.startsWith(">")){
                if (queryPart.endsWith("<")){
                    //contains query
                    this.queryType = QueryType.CONTAINS;
                    this.query = queryPart.substring(1, queryPart.length() - 1);
                }else{
                    //ends with query
                    this.queryType = QueryType.ENDS_WITH;
                    this.query = queryPart.substring(1);
                }
            }else{
                if (queryPart.endsWith("<")){
                    //starts with query
                    this.queryType = QueryType.STARTS_WITH;
                    this.query = queryPart.substring(0, queryPart.length() - 1);
                }else{
                    //exact match query
                    this.queryType = QueryType.MATCHES;
                    this.query = queryPart;
                }
            }
        }

        private static enum QueryType{
            STARTS_WITH, ENDS_WITH, CONTAINS, MATCHES
        }
    }
}
