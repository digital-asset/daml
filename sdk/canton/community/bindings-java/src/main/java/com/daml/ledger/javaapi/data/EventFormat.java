// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TransactionFilterOuterClass;
import com.daml.ledger.javaapi.data.codegen.ContractCompanion;
import com.daml.ledger.javaapi.data.codegen.ContractTypeCompanion;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class EventFormat {

  private final Map<String, Filter> partyToFilters;
  private final Optional<Filter> anyPartyFilterO;
  private final boolean verbose;

  public static EventFormat fromProto(TransactionFilterOuterClass.EventFormat eventFormat) {
    Map<String, TransactionFilterOuterClass.Filters> partyToFilters =
        eventFormat.getFiltersByPartyMap();
    HashMap<String, Filter> converted = new HashMap<>(partyToFilters.size());
    for (Map.Entry<String, TransactionFilterOuterClass.Filters> entry : partyToFilters.entrySet()) {
      converted.put(entry.getKey(), Filter.fromProto(entry.getValue()));
    }

    TransactionFilterOuterClass.Filters anyPartyFilters = eventFormat.getFiltersForAnyParty();
    Filter convertedAnyPartyFilter = Filter.fromProto(anyPartyFilters);

    Optional<Filter> anyPartyFilterO =
        (convertedAnyPartyFilter instanceof NoFilter)
            ? Optional.empty()
            : Optional.of(convertedAnyPartyFilter);

    return new EventFormat(converted, anyPartyFilterO, eventFormat.getVerbose());
  }

  public TransactionFilterOuterClass.EventFormat toProto() {
    HashMap<String, TransactionFilterOuterClass.Filters> partyToFilters =
        new HashMap<>(this.partyToFilters.size());
    for (Map.Entry<String, Filter> entry : this.partyToFilters.entrySet()) {
      partyToFilters.put(entry.getKey(), entry.getValue().toProto());
    }

    TransactionFilterOuterClass.EventFormat.Builder builder =
        TransactionFilterOuterClass.EventFormat.newBuilder().putAllFiltersByParty(partyToFilters);

    this.anyPartyFilterO.ifPresent(value -> builder.setFiltersForAnyParty(value.toProto()));

    builder.setVerbose(verbose);

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

  public EventFormat(
      @NonNull Map<@NonNull String, @NonNull Filter> partyToFilters,
      @NonNull Optional<@NonNull Filter> anyPartyFilterO,
      boolean verbose) {
    this.partyToFilters = partyToFilters;
    this.anyPartyFilterO = anyPartyFilterO;
    this.verbose = verbose;
  }

  public boolean getVerbose() {
    return this.verbose;
  }

  public static EventFormat eventFormat(
      ContractTypeCompanion<?, ?, ?, ?> contractCompanion,
      Optional<Set<String>> partiesO,
      boolean verbose) {
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

    return new EventFormat(partyToFilters, anyPartyFilterO, verbose);
  }

  @Override
  public String toString() {
    return "EventFormat{"
        + "partyToFilters="
        + partyToFilters
        + ", anyPartyFilterO="
        + anyPartyFilterO
        + ", verbose="
        + verbose
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EventFormat that = (EventFormat) o;
    return verbose == that.verbose
        && Objects.equals(partyToFilters, that.partyToFilters)
        && Objects.equals(anyPartyFilterO, that.anyPartyFilterO);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partyToFilters, anyPartyFilterO, verbose);
  }
}
