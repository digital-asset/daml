// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.javaapi.data.codegen.Contract;
import com.daml.ledger.javaapi.data.codegen.ContractCompanion;
import com.daml.ledger.javaapi.data.codegen.ContractTypeCompanion;
import com.daml.ledger.javaapi.data.codegen.InterfaceCompanion;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class contains utilities to decode a <code>CreatedEvent</code> and create a <code>
 * TransactionFilter</code> by provider parties It can only be instantiated with a subtype of <code>
 * ContractCompanion</code>
 */
public final class ContractFilter<Ct> {
  private final ContractTypeCompanion<Ct, ?, ?, ?> companion;

  private final CumulativeFilter filter;

  private ContractFilter(ContractTypeCompanion<Ct, ?, ?, ?> companion, CumulativeFilter filter) {
    this.companion = companion;
    this.filter = filter;
  }

  public static <Ct> ContractFilter<Ct> of(ContractCompanion<Ct, ?, ?> companion) {
    CumulativeFilter filter =
        new CumulativeFilter(
            Collections.emptyMap(),
            Collections.singletonMap(
                companion.TEMPLATE_ID, Filter.Template.HIDE_CREATED_EVENT_BLOB),
            Optional.empty());
    return new ContractFilter<>(companion, filter);
  }

  public static <Cid, View> ContractFilter<Contract<Cid, View>> of(
      InterfaceCompanion<?, Cid, View> companion) {
    CumulativeFilter filter =
        new CumulativeFilter(
            Collections.singletonMap(
                companion.TEMPLATE_ID, Filter.Interface.INCLUDE_VIEW_HIDE_CREATED_EVENT_BLOB),
            Collections.emptyMap(),
            Optional.empty());
    return new ContractFilter<>(companion, filter);
  }

  public ContractFilter<Ct> withIncludeCreatedEventBlob(boolean includeCreatedEventBlob) {
    Filter.Interface interfaceFilterConfig =
        includeCreatedEventBlob
            ? Filter.Interface.INCLUDE_VIEW_INCLUDE_CREATED_EVENT_BLOB
            : Filter.Interface.INCLUDE_VIEW_HIDE_CREATED_EVENT_BLOB;

    Filter.Template templateFilterConfig =
        includeCreatedEventBlob
            ? Filter.Template.INCLUDE_CREATED_EVENT_BLOB
            : Filter.Template.HIDE_CREATED_EVENT_BLOB;

    Map<@NonNull Identifier, Filter.Interface> interfaceFiltersWithCreatedEventBlob =
        filter.getInterfaceFilters().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, x -> interfaceFilterConfig));

    Map<@NonNull Identifier, Filter.Template> templateFiltersWithCreatedEventBlob =
        filter.getTemplateFilters().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, x -> templateFilterConfig));

    Optional<Filter.@NonNull Wildcard> includeCreatedEventBlobWildcard =
        filter
            .getWildcardFilter()
            .map(
                w ->
                    includeCreatedEventBlob
                        ? Filter.Wildcard.INCLUDE_CREATED_EVENT_BLOB
                        : Filter.Wildcard.HIDE_CREATED_EVENT_BLOB);

    CumulativeFilter filterWithIncludedCreatedEventBlob =
        new CumulativeFilter(
            interfaceFiltersWithCreatedEventBlob,
            templateFiltersWithCreatedEventBlob,
            includeCreatedEventBlobWildcard);

    return new ContractFilter<>(companion, filterWithIncludedCreatedEventBlob);
  }

  public Ct toContract(CreatedEvent createdEvent) throws IllegalArgumentException {
    return companion.fromCreatedEvent(createdEvent);
  }

  public TransactionFilter transactionFilter(Optional<Set<String>> parties) {
    return transactionFilter(filter, parties);
  }

  private static TransactionFilter transactionFilter(
      Filter filter, Optional<Set<String>> partiesO) {
    Map<String, Filter> partyToFilters =
        partiesO
            .map(
                parties ->
                    parties.stream().collect(Collectors.toMap(Function.identity(), x -> filter)))
            .orElse(Collections.emptyMap());

    Optional<Filter> anyPartyFilterO = partiesO.isEmpty() ? Optional.of(filter) : Optional.empty();

    return new TransactionFilter(partyToFilters, anyPartyFilterO);
  }
}
