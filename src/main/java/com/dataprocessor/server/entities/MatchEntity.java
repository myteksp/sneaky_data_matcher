package com.dataprocessor.server.entities;

import java.util.Objects;

public final class MatchEntity {
    public String name;
    public long processed;
    public long outOf;
    public boolean completed;
    public long timeStamp;

    public MatchEntity(){}
    public MatchEntity(final String name,
                       final long processed,
                       final long outOf,
                       final boolean completed,
                       final long timeStamp){
        this.name = name;
        this.processed = processed;
        this.outOf = outOf;
        this.completed = completed;
        this.timeStamp = timeStamp;
    }

    @Override
    public final boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final MatchEntity that = (MatchEntity) o;
        return processed == that.processed && outOf == that.outOf && completed == that.completed && timeStamp == that.timeStamp && Objects.equals(name, that.name);
    }
    @Override
    public final int hashCode() {
        return Objects.hash(name, processed, outOf, completed, timeStamp);
    }
    @Override
    public final String toString() {
        return "MatchEntity{" +
                "name='" + name + '\'' +
                ", processed=" + processed +
                ", outOf=" + outOf +
                ", completed=" + completed +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
