package com.dataprocessor.server.utils;

import java.util.UUID;

public final class StringUtil {
    public static final boolean isNullOrBlank(final String string){
        if (string == null)
            return true;

        return string.isBlank();
    }

    public static final String generateId(){
        return UUID.randomUUID().toString().replace("-", "");
    }
}
