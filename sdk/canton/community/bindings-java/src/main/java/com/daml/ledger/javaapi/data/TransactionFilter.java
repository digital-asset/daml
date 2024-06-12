// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TransactionFilterOuterClass;
import com.daml.ledger.javaapi.data.codegen.ContractCompanion;
import com.daml.ledger.javaapi.data.codegen.ContractTypeCompanion;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class TransactionFilter {

  private Map<String, Filter> partyToFilters;
  private Optional<Filter> anyPartyFilterO;

  public static TransactionFilter fromProto(
      TransactionFilterOuterClass.TransactionFilter transactionFilter) {
    Map<String, TransactionFilterOuterClass.Filters> partyToFilters =
        transactionFilter.getFiltersByPartyMap();
    HashMap<String, Filter> converted = new HashMap<>(partyToFilters.size());
    for (Map.Entry<String, TransactionFilterOuterClass.Filters> entry : partyToFilters.entrySet()) {
      converted.put(entry.getKey(), Filter.fromProto(entry.getValue()));
    }

    TransactionFilterOuterClass.Filters anyPartyFilters = transactionFilter.getFiltersForAnyParty();
    Filter convertedAnyPartyFilter = Filter.fromProto(anyPartyFilters);

    Optional<Filter> anyPartyFilterO =
        (convertedAnyPartyFilter instanceof NoFilter)
            ? Optional.empty()
            : Optional.of(convertedAnyPartyFilter);

    return new TransactionFilter(converted, anyPartyFilterO);
  }

  public TransactionFilterOuterClass.TransactionFilter toProto() {
    HashMap<String, TransactionFilterOuterClass.Filters> partyToFilters =
        new HashMap<>(this.partyToFilters.size());
    for (Map.Entry<String, Filter> entry : this.partyToFilters.entrySet()) {
      partyToFilters.put(entry.getKey(), entry.getValue().toProto());
    }

    TransactionFilterOuterClass.TransactionFilter.Builder builder =
        TransactionFilterOuterClass.TransactionFilter.newBuilder()
            .putAllFiltersByParty(partyToFilters);

    this.anyPartyFilterO.ifPresent(value -> builder.setFiltersForAnyParty(value.toProto()));

    return builder.build();
  }

  public Set<String> getParties() {
    return partyToFilters.keySet();
  }

  public Map<String, Filter> getPartyToFilters() {
    return partyToFilters;
  }

  public Optional<Filter> getAnyPartyFilter() {
    return anyPartyFilterO;
  }

  public TransactionFilter(
      @NonNull Map<@NonNull String, @NonNull Filter> partyToFilters,
      @NonNull Optional<@NonNull Filter> anyPartyFilterO) {
    this.partyToFilters = partyToFilters;
    this.anyPartyFilterO = anyPartyFilterO;
  }

  public TransactionFilter(
      @NonNull Map<@NonNull String, @NonNull Filter> partyToFilters) {
    this.partyToFilters = partyToFilters;
    this.anyPartyFilterO = Optional.empty();
  }

  public static TransactionFilter transactionFilter(
      ContractTypeCompanion<?, ?, ?, ?> contractCompanion, Optional<Set<String>> partiesO) {
    Filter filter =
        (contractCompanion instanceof ContractCompanion)
            ? new CumulativeFilter(
                Collections.emptyMap(),
                Collections.singletonMap(
                    contractCompanion.TEMPLATE_ID, Filter.Template.HIDE_CREATED_EVENT_BLOB),
                Optional.empty())
            : new CumulativeFilter(
                Map.of(
                    contractCompanion.TEMPLATE_ID,
                    Filter.Interface.INCLUDE_VIEW_HIDE_CREATED_EVENT_BLOB),
                Collections.emptyMap(),
                Optional.empty());

    Map<String, Filter> partyToFilters =
        partiesO
            .map(
                parties ->
                    parties.stream().collect(Collectors.toMap(Function.identity(), x -> filter)))
            .orElse(Map.of());

    Optional<Filter> anyPartyFilterO = partiesO.isEmpty() ? Optional.of(filter) : Optional.empty();

    return new TransactionFilter(partyToFilters, anyPartyFilterO);
  }

  @Override
  public String toString() {
    return "TransactionFilter{"
        + "partyToFilters="
        + partyToFilters
        + ",anyPartyFilterO="
        + anyPartyFilterO
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TransactionFilter that = (TransactionFilter) o;
    return Objects.equals(partyToFilters, that.partyToFilters)
        && Objects.equals(anyPartyFilterO, that.anyPartyFilterO);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partyToFilters, anyPartyFilterO);
  }
}
