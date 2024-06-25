package com.dataprocessor.server.utils;

import java.util.List;
import java.util.StringTokenizer;

public class StringTransformer {
    public static enum Transformation{
        TLC, TUC, TRIM, NRM
    }

    public static final String transform(final String src, final List<Transformation> transformations){
        String result = src==null?"":src;
        for (final Transformation transformation : transformations) {
            switch (transformation) {
                case TLC -> result = result.toLowerCase();
                case TUC -> result = result.toUpperCase();
                case TRIM -> result = result.trim();
                case NRM -> result = normalize(result);
            }
        }
        return result;
    }

    private static final String trimQuotes(final String string){
        String res = string.trim();
        while (res.startsWith("'") || res.startsWith("\"")){
            res = res.substring(1).trim();
        }
        while (res.endsWith("'") || res.endsWith("\"")){
            res = res.substring(0, res.length()-1).trim();
        }
        return res;
    }

    private static final String normalize(final String str){
        final StringTokenizer tokenizer = new StringTokenizer(str);
        final StringBuilder sb = new StringBuilder(str.length());
        while (tokenizer.hasMoreTokens()) {
            final String token = trimQuotes(tokenizer.nextToken().trim());
            if (!token.isBlank()){
                sb.append(token).append(' ');
            }
        }
        return sb.toString().trim();
    }
}
