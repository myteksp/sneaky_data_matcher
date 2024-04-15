package com.dataprocessor.server.entities;

import java.util.List;
import java.util.Objects;

public final class UploadDescriptor {
    public String name;
    public long processed;
    public long outOf;
    public long timeStamp;
    public List<UploadMapping> mappings;
    public Status status;
    public UploadDescriptor(){}
    public UploadDescriptor(final String name,
                            final long processed,
                            final long outOf,
                            final long timeStamp,
                            final List<UploadMapping> mappings,
                            final Status status){
        this.name = name;
        this.processed = processed;
        this.outOf = outOf;
        this.timeStamp = timeStamp;
        this.mappings = mappings;
        this.status = status;
    }

    @Override
    public final boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UploadDescriptor that = (UploadDescriptor) o;
        return processed == that.processed && outOf == that.outOf && timeStamp == that.timeStamp && Objects.equals(name, that.name) && Objects.equals(mappings, that.mappings) && status == that.status;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(name, processed, outOf, timeStamp, mappings, status);
    }

    @Override
    public final String toString() {
        return "UploadDescriptor{" +
                "name='" + name + '\'' +
                ", processed=" + processed +
                ", outOf=" + outOf +
                ", timeStamp=" + timeStamp +
                ", mappings=" + mappings +
                ", status=" + status +
                '}';
    }

    public static enum Status{
        PROCESSING, FINISHED, FINISHED_WITH_ERROR
    }

}
