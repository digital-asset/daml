// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TransactionFilterOuterClass;
import com.daml.ledger.api.v2.TransactionFilterOuterClass.Filters;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class FiltersByParty extends TransactionFilter {

  private Map<String, Filter> partyToFilters;

  @Override
  public Set<String> getParties() {
    return partyToFilters.keySet();
  }

  public FiltersByParty(@NonNull Map<@NonNull String, @NonNull Filter> partyToFilters) {
    this.partyToFilters = partyToFilters;
  }

  @Override
  public TransactionFilterOuterClass.TransactionFilter toProto() {
    HashMap<String, Filters> partyToFilters = new HashMap<>(this.partyToFilters.size());
    for (Map.Entry<String, Filter> entry : this.partyToFilters.entrySet()) {
      partyToFilters.put(entry.getKey(), entry.getValue().toProto());
    }
    return TransactionFilterOuterClass.TransactionFilter.newBuilder()
        .putAllFiltersByParty(partyToFilters)
        .build();
  }

  public static FiltersByParty fromProto(
      TransactionFilterOuterClass.TransactionFilter transactionFilter) {
    Map<String, Filters> partyToFilters = transactionFilter.getFiltersByPartyMap();
    HashMap<String, Filter> converted = new HashMap<>(partyToFilters.size());
    for (Map.Entry<String, Filters> entry : partyToFilters.entrySet()) {
      converted.put(entry.getKey(), Filter.fromProto(entry.getValue()));
    }
    return new FiltersByParty(converted);
  }

  public Map<String, Filter> getPartyToFilters() {
    return partyToFilters;
  }

  @Override
  public String toString() {
    return "FiltersByParty{" + "partyToFilters=" + partyToFilters + '}';
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
