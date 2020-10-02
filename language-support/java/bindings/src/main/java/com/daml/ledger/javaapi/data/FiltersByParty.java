// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.TransactionFilterOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class FiltersByParty extends TransactionFilter {

    private Map<String, Filter> partyToFilters;

    @Override
    public Set<String> getParties() {
        return partyToFilters.keySet();
    }

    public FiltersByParty(@NonNull Map<@NonNull String, @NonNull Filter> partyToFilters) {
        this.partyToFilters = partyToFilters;
    }

    @Override
    TransactionFilterOuterClass.TransactionFilter toProto() {
        HashMap<String, TransactionFilterOuterClass.Filters> partyToFilters = new HashMap<>(this.partyToFilters.size());
        for (String party : this.partyToFilters.keySet()) {
            partyToFilters.put(party, this.partyToFilters.get(party).toProto());
        }
        return TransactionFilterOuterClass.TransactionFilter.newBuilder()
                .putAllFiltersByParty(partyToFilters)
                .build();
    }

    public static FiltersByParty fromProto(TransactionFilterOuterClass.TransactionFilter transactionFilter) {
        Map<String, TransactionFilterOuterClass.Filters> partyToFilters = transactionFilter.getFiltersByPartyMap();
        HashMap<String, Filter> converted = new HashMap<>(partyToFilters.size());
        for (String party : partyToFilters.keySet()) {
            converted.put(party, Filter.fromProto(partyToFilters.get(party)));
        }
        return new FiltersByParty(converted);
    }

    public Map<String, Filter> getPartyToFilters() {
        return partyToFilters;
    }

    @Override
    public String toString() {
        return "FiltersByParty{" +
                "partyToFilters=" + partyToFilters +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FiltersByParty that = (FiltersByParty) o;
        return Objects.equals(partyToFilters, that.partyToFilters);
    }

    @Override
    public int hashCode() {

        return Objects.hash(partyToFilters);
    }
}
