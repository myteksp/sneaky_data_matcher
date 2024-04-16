package com.dataprocessor.server.entities;

import com.dataprocessor.server.utils.StringTransformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class UploadMapping {
    public List<String> sourceColumns;
    public String destinationColumn;
    public List<StringTransformer.Transformation> transformations;

    public UploadMapping(){}
    public UploadMapping(final List<String> sourceColumns,
                         final String destinationColumn,
                         final List<StringTransformer.Transformation> transformations){
        this.sourceColumns = sourceColumns;
        this.destinationColumn = destinationColumn;
        this.transformations = transformations;
    }

    @Override
    public final boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final UploadMapping that = (UploadMapping) o;
        return Objects.equals(sourceColumns, that.sourceColumns) && Objects.equals(destinationColumn, that.destinationColumn) && Objects.equals(transformations, that.transformations);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(sourceColumns, destinationColumn, transformations);
    }

    @Override
    public final String toString() {
        return "UploadMapping{" +
                "sourceColumns=" + sourceColumns +
                ", destinationColumn='" + destinationColumn + '\'' +
                ", transformations=" + transformations +
                '}';
    }

    public static final class UploadMappingList extends ArrayList<UploadMapping> implements List<UploadMapping>{}
}
