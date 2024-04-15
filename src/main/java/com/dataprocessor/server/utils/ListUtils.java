package com.dataprocessor.server.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class ListUtils {
    public static final List<String> listOfString(final String ...args){
        final List<String> res = new ArrayList<>(args.length + 20);
        res.addAll(Arrays.asList(args));
        return res;
    }

    public static final <T> List<T> ifNullEmpty(final List<T> list){
        if (list == null)
            return new ArrayList<>(16);

        return list;
    }

    public static final <T> T getFirst(final Iterable<T> iterable){
        for(final T it : iterable)
            return it;

        return null;
    }
    public static final <T> T getFirst(final Iterable<T> iterable, final T defaultValue){
        for(final T it : iterable)
            return it;

        return defaultValue;
    }
}
