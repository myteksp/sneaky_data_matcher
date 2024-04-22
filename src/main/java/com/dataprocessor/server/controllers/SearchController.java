package com.dataprocessor.server.controllers;

import com.dataprocessor.server.entities.*;
import com.dataprocessor.server.services.SearchService;
import com.dataprocessor.server.utils.ListUtils;
import com.dataprocessor.server.utils.TempFileUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/data")
public class SearchController {
    private final SearchService service;

    @Autowired
    public SearchController(final SearchService service){
        this.service = service;
    }

    @GetMapping(value = "/searchForField", produces = MediaType.APPLICATION_JSON_VALUE)
    public final List<Map<String, List<String>>> searchForField(@RequestParam(value = "columnAndQuery", required = false)final List<String> columnSearches,
                                                                @RequestParam(value = "predicate", required = false, defaultValue = "AND") final LogicalPredicate predicate,
                                                                @RequestParam(value = "uploads", required = false) final List<String> limitByUploads,
                                                                @RequestParam(value = "joinOn", required = false)final String joinByColumn,
                                                                @RequestParam(value = "field", required = true)final String field,
                                                                @RequestParam(value = "maxDepth", defaultValue = "2", required = false) final int maxJoinDepth,
                                                                @RequestParam(value = "skip", defaultValue = "0", required = false) final int skip,
                                                                @RequestParam(value = "limit", defaultValue = "10", required = false) final int limit){
        return service.searchForField(
                ListUtils.ifNullEmpty(columnSearches),
                predicate,
                ListUtils.ifNullEmpty(limitByUploads),
                joinByColumn,
                field,
                maxJoinDepth, skip, limit);
    }

    @GetMapping(value = "/search", produces = MediaType.APPLICATION_JSON_VALUE)
    public final List<Map<String, List<String>>> search(@RequestParam(value = "columnAndQuery", required = false)final List<String> columnSearches,
                                                        @RequestParam(value = "predicate", required = false, defaultValue = "AND") final LogicalPredicate predicate,
                                                        @RequestParam(value = "uploads", required = false) final List<String> limitByUploads,
                                                        @RequestParam(value = "joinOn", required = false)final List<String> joinByColumns,
                                                        @RequestParam(value = "maxDepth", defaultValue = "2", required = false) final int maxJoinDepth,
                                                        @RequestParam(value = "skip", defaultValue = "0", required = false) final int skip,
                                                        @RequestParam(value = "limit", defaultValue = "10", required = false) final int limit){

        return service.search(
                ListUtils.ifNullEmpty(columnSearches),
                predicate,
                ListUtils.ifNullEmpty(limitByUploads),
                ListUtils.ifNullEmpty(joinByColumns),
                maxJoinDepth, skip, limit);
    }


    @GetMapping(value = "/searchAndExport", produces = MediaType.APPLICATION_JSON_VALUE)
    public final GenericResponse searchAndExport(@RequestParam(value = "columnAndQuery", required = false)final List<String> columnSearches,
                                                 @RequestParam(value = "predicate", required = false, defaultValue = "AND") final LogicalPredicate predicate,
                                                 @RequestParam(value = "uploads", required = false) final List<String> limitByUploads,
                                                 @RequestParam(value = "joinOn", required = false)final List<String> joinByColumns,
                                                 @RequestParam(value = "maxDepth", defaultValue = "2", required = false) final int maxJoinDepth,
                                                 @RequestParam(value = "destination")final String exportDestination){
        service.searchAndExport(ListUtils.ifNullEmpty(columnSearches),
                predicate,
                ListUtils.ifNullEmpty(limitByUploads),
                ListUtils.ifNullEmpty(joinByColumns),
                maxJoinDepth, exportDestination);
        return GenericResponse.builder()
                .success(true)
                .message("Search and export started. Destination file: '" + exportDestination + "'.")
                .build();
    }


    @PostMapping(value = "/matchAndExport", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public final MatchEntity matchAndExport(@RequestParam("file") final MultipartFile file,
                                            @RequestParam("mappings") final List<String> mappings,
                                            @RequestParam(value = "predicate", required = false, defaultValue = "AND") final LogicalPredicate predicate,
                                            @RequestParam(value = "uploads", required = false) final List<String> limitByUploads,
                                            @RequestParam(value = "joinOn", required = false)final List<String> joinByColumns,
                                            @RequestParam(value = "maxDepth", defaultValue = "2", required = false) final int maxJoinDepth,
                                            @RequestParam(value = "destination")final String exportDestination){
        return service.matchAndExport(TempFileUtil.copyToTmpFile(file),
                ListUtils.ifNullEmpty(mappings),
                predicate,
                ListUtils.ifNullEmpty(limitByUploads),
                ListUtils.ifNullEmpty(joinByColumns),
                maxJoinDepth, exportDestination);
    }

    @GetMapping(value = "/getMatch", produces = MediaType.APPLICATION_JSON_VALUE)
    public final MatchEntity getExistingMatch(@RequestParam("fileName") final String name){
        return service.getMatch(name);
    }

    @GetMapping(value = "/forcefullyCompleteMatch", produces = MediaType.APPLICATION_JSON_VALUE)
    public final MatchEntity forcefullyCompleteMatch(@RequestParam("name") final String name){
        return service.forcefullyCompleteMatch(name);
    }

    @GetMapping(value = "/listMatches", produces = MediaType.APPLICATION_JSON_VALUE)
    public final List<MatchEntity> listMatches(@RequestParam(value = "skip", defaultValue = "0")final int skip,
                                               @RequestParam(value = "limit", defaultValue = "100")final int limit){
        return service.listMatches(skip, limit);
    }


    @GetMapping(value = "/getColumns", produces = MediaType.APPLICATION_JSON_VALUE)
    public final List<String> getMappings(){
        return service.getMappings();
    }

    @GetMapping(value = "/getSearch", produces = MediaType.APPLICATION_JSON_VALUE)
    public final SearchEntity getSearch(@RequestParam("name") final String name){
        return service.getSearch(name);
    }
}
