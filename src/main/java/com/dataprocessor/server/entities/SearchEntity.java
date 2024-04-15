package com.dataprocessor.server.entities;

import java.util.List;
import java.util.Objects;

public final class SearchEntity {
    public List<String> columnSearches;
    public LogicalPredicate predicate;
    public List<String> limitByUploads;
    public List<String> joinByColumns;
    public int maxJoinDepth;

    public SearchEntity(final List<String> columnSearches,
                        final LogicalPredicate predicate,
                        final List<String> limitByUploads,
                        final List<String> joinByColumns,
                        final int maxJoinDepth){
        this.columnSearches = columnSearches;
        this.predicate = predicate;
        this.limitByUploads = limitByUploads;
        this.joinByColumns = joinByColumns;
        this.maxJoinDepth = maxJoinDepth;
    }
    public SearchEntity(){}

    @Override
    public final boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final SearchEntity that = (SearchEntity) o;
        return maxJoinDepth == that.maxJoinDepth && Objects.equals(columnSearches, that.columnSearches) && predicate == that.predicate && Objects.equals(limitByUploads, that.limitByUploads) && Objects.equals(joinByColumns, that.joinByColumns);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(columnSearches, predicate, limitByUploads, joinByColumns, maxJoinDepth);
    }

    @Override
    public final String toString() {
        return "SearchEntity{" +
                "columnSearches=" + columnSearches +
                ", predicate=" + predicate +
                ", limitByUploads=" + limitByUploads +
                ", joinByColumns=" + joinByColumns +
                ", maxJoinDepth=" + maxJoinDepth +
                '}';
    }
}
