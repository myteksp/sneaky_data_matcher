package com.dataprocessor.server.utils;


import com.dataprocessor.server.utils.json.JSON;
import com.dataprocessor.server.utils.tuples.Tuple2;
import jakarta.xml.bind.DatatypeConverter;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.security.MessageDigest;
import java.util.List;

public final class Signature {
    private static final byte[] null_bytes = new byte[]{};
    public static final String getSignature(final Object object)  {
        final MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        }catch (final Throwable cause){
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to generate hash", cause);
        }
        if (object == null) {
            md.update(null_bytes);
        }else{
            md.update(JSON.toJson(object).getBytes());
        }
        return DatatypeConverter.printHexBinary(md.digest());
    }
}
