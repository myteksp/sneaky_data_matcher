package com.dataprocessor.server.entities;

import java.util.Objects;

public class GenericResponse {
    public boolean success;
    public String message;

    public GenericResponse() {}
    public GenericResponse(final boolean success, final String message){
        this.success = success;
        this.message = message;
    }

    @Override
    public final boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final GenericResponse that = (GenericResponse) o;
        return success == that.success && Objects.equals(message, that.message);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(success, message);
    }

    @Override
    public final String toString() {
        return "GenericResponse{" +
                "success=" + success +
                ", message='" + message + '\'' +
                '}';
    }

    public static final Builder builder(){
        return new Builder();
    }
    public static final class Builder{
        private final GenericResponse result;
        private Builder(){
            this.result = new GenericResponse();
        }
        public final Builder success(final boolean success){
            result.success = success;
            return this;
        }
        public final Builder message(final String message){
            result.message = message;
            return this;
        }
        public final GenericResponse build(){
            return result;
        }
    }
}
