package com.dataprocessor.server.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public final class ListUtils {

    public static final <T> List<T> arrayToList(final T[] arr){
        final ArrayList<T> result = new ArrayList<>(arr.length + 8);
        result.addAll(Arrays.asList(arr));
        return result;
    }
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

    public static final int sumOfLengths(final List listOfLists){
        int res = 0;
        for(final Object l : listOfLists) {
            if (l instanceof Collection<?>){
                res += ((Collection<?>)l).size();
            }
        }
        return res;
    }
}
