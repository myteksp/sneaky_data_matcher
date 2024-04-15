package com.dataprocessor.server.utils.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public final class JSON {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectMapper prettyMapper = new ObjectMapper();
    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

    static{
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        prettyMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        prettyMapper.enable(SerializationFeature.INDENT_OUTPUT).enable(JsonGenerator.Feature.IGNORE_UNKNOWN);
        yamlMapper.findAndRegisterModules();
    }

    public static final String toYaml(final Object obj){
        return wrapIntoRuntimeException(()->yamlMapper.writeValueAsString(obj));
    }

    public static final String toJson(final Object obj){
        return wrapIntoRuntimeException(()->mapper.writeValueAsString(obj));
    }

    public static final String toPrettyJson(final Object obj){
        return wrapIntoRuntimeException(()->prettyMapper.writeValueAsString(obj));
    }

    public static final <T> T fromYaml(final String yaml, final Class<T> valueType){
        return read(yamlMapper, yaml, valueType);
    }

    public static final <T> T fromJson(final String json, final Class<T> valueType){
        return read(mapper, json, valueType);
    }

    private static final <T> T read(final ObjectMapper map, final String value, final Class<T> valueType){
        if (value == null) {
            return null;
        }
        if (valueType == null){
            throw new NullPointerException("valueType parameter can not be null");
        }
        return wrapIntoRuntimeException(()->map.readValue(value, valueType));
    }


    //Exceptions wrapper
    private static interface ExecutionWrapper<O>{
        O execute() throws Throwable;
    }

    public static final class JsonException extends RuntimeException{
        public JsonException(final Throwable throwable){
            super("Json parse error", throwable);
        }
    }

    private static final <O> O wrapIntoRuntimeException(final ExecutionWrapper<O> wrapper){
        try { return wrapper.execute(); }catch (final Throwable error){ throw new JsonException(error); }
    }
}
