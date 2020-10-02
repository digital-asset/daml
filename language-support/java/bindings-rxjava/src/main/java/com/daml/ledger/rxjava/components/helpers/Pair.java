// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.components.helpers;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.function.Function;

public class Pair<F, S> {

    private final F first;
    private final S second;

    public Pair(@NonNull F first, @NonNull S second) {
        this.first = first;
        this.second = second;
    }

    @NonNull public final F getFirst() { return first; }

    @NonNull public final S getSecond() { return second; }

    @NonNull public final <F2> Pair<F2, S> mapFirst(Function<F, F2> f) {
        return new Pair<>(f.apply(getFirst()), getSecond());
    }

    @NonNull public final <S2> Pair<F, S2> mapSecond(Function<S, S2> f) {
        return new Pair<>(getFirst(), f.apply(getSecond()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(first, pair.first) &&
                Objects.equals(second, pair.second);
    }

    @Override
    public int hashCode() {

        return Objects.hash(first, second);
    }

    @Override
    public String toString() {
        return "Pair{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }
}
