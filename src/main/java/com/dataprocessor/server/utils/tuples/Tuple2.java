package com.dataprocessor.server.utils.tuples;

import java.util.Objects;

public class Tuple2 <T1, T2>{
    public final T1 v1;
    public final T2 v2;

    public Tuple2(final T1 v1, final T2 v2){
        this.v1 = v1;
        this.v2 = v2;
    }

    @Override
    public final boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Tuple2<?, ?> tuple2 = (Tuple2<?, ?>) o;
        return Objects.equals(v1, tuple2.v1) && Objects.equals(v2, tuple2.v2);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(v1, v2);
    }

    @Override
    public final String toString() {
        return "Tuple2{" +
                "v1=" + v1 +
                ", v2=" + v2 +
                '}';
    }
}
