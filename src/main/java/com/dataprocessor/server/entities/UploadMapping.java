package com.dataprocessor.server.entities;

import com.dataprocessor.server.utils.StringTransformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class UploadMapping {
    public String sourceColumn;
    public String destinationColumn;
    public List<StringTransformer.Transformation> transformations;

    public UploadMapping(){}
    public UploadMapping(final String sourceColumn,
                         final String destinationColumn,
                         final List<StringTransformer.Transformation> transformations){
        this.sourceColumn = sourceColumn;
        this.destinationColumn = destinationColumn;
        this.transformations = transformations;
    }

    @Override
    public final boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final UploadMapping that = (UploadMapping) o;
        return Objects.equals(sourceColumn, that.sourceColumn) && Objects.equals(destinationColumn, that.destinationColumn) && Objects.equals(transformations, that.transformations);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(sourceColumn, destinationColumn, transformations);
    }

    @Override
    public final String toString() {
        return "UploadMapping{" +
                "sourceColumn='" + sourceColumn + '\'' +
                ", destinationColumn='" + destinationColumn + '\'' +
                ", transformations=" + transformations +
                '}';
    }

    public static final class UploadMappingList extends ArrayList<UploadMapping> implements List<UploadMapping>{}
}
