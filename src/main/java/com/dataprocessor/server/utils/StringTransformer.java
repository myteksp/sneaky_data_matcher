package com.dataprocessor.server.utils;

import java.util.List;
import java.util.StringTokenizer;

public class StringTransformer {
    public static enum Transformation{
        TLC, TUC, TRIM, NRM
    }

    public static final String transform(final String src, final List<Transformation> transformations){
        String result = src;
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

    private static final String normalize(final String str){
        final StringTokenizer tokenizer = new StringTokenizer(str);
        final StringBuilder sb = new StringBuilder(str.length());
        while (tokenizer.hasMoreTokens()) {
            final String token = tokenizer.nextToken().trim();
            if (!token.isBlank()){
                sb.append(token).append(' ');
            }
        }
        return sb.toString().trim();
    }
}
