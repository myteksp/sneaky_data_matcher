package com.dataprocessor.server.utils;

import com.dataprocessor.server.entities.UploadMapping;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class UploadMappingUtil {
    public static final List<UploadMapping> parse(final List<String> mappings){
        final int size = mappings.size();
        if (size < 1){
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Mappings are empty");
        }
        final List<UploadMapping> result = new ArrayList<>(size);
        for(final String mapping : mappings){
            result.add(parse(mapping));
        }
        return result;
    }
    public static final UploadMapping parse(final String mapping){
        final String[] split = mapping.split(":");
        if (split.length < 2){
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid mapping: '" + mapping + "'.");
        }
        final List<StringTransformer.Transformation> transformations = new ArrayList<>(5);
        for (int i = 2; i < split.length; i++) {
            try {
                transformations.add(StringTransformer.Transformation.valueOf(split[i].toUpperCase()));
            }catch (final Throwable t){
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid mapping: '" + mapping + "'.");
            }
        }
        return new UploadMapping(
                ListUtils.arrayToList(split[0].split("\\|")),
                StringTransformer.transform(split[1], List.of(StringTransformer.Transformation.TLC, StringTransformer.Transformation.NRM)),
                transformations);
    }

}
