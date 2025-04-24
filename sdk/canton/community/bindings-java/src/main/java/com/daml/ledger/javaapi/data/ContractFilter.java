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
  private final boolean verbose;
  private final TransactionShape transactionShape;

  private ContractFilter(
      ContractTypeCompanion<Ct, ?, ?, ?> companion,
      CumulativeFilter filter,
      boolean verbose,
      TransactionShape transactionShape) {
    this.companion = companion;
    this.filter = filter;
    this.verbose = verbose;
    this.transactionShape = transactionShape;
  }

  public static <Ct> ContractFilter<Ct> of(ContractCompanion<Ct, ?, ?> companion) {
    CumulativeFilter filter =
        new CumulativeFilter(
            Collections.emptyMap(),
            Collections.singletonMap(
                companion.TEMPLATE_ID, Filter.Template.HIDE_CREATED_EVENT_BLOB),
            Optional.empty());
    return new ContractFilter<>(companion, filter, false, TransactionShape.ACS_DELTA);
  }

  public static <Cid, View> ContractFilter<Contract<Cid, View>> of(
      InterfaceCompanion<?, Cid, View> companion) {
    CumulativeFilter filter =
        new CumulativeFilter(
            Collections.singletonMap(
                companion.TEMPLATE_ID, Filter.Interface.INCLUDE_VIEW_HIDE_CREATED_EVENT_BLOB),
            Collections.emptyMap(),
            Optional.empty());
    return new ContractFilter<>(companion, filter, false, TransactionShape.ACS_DELTA);
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

    return new ContractFilter<>(
        companion, filterWithIncludedCreatedEventBlob, verbose, transactionShape);
  }

  public ContractFilter<Ct> withVerbose(boolean verbose) {
    return new ContractFilter<>(companion, filter, verbose, transactionShape);
  }

  public ContractFilter<Ct> withTransactionShape(TransactionShape transactionShape) {
    return new ContractFilter<>(companion, filter, verbose, transactionShape);
  }

  public Ct toContract(CreatedEvent createdEvent) throws IllegalArgumentException {
    return companion.fromCreatedEvent(createdEvent);
  }

  /** Method will be removed in 3.4.0 */
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

  public UpdateFormat updateFormat(Optional<Set<String>> parties) {
    return updateFormat(filter, parties, verbose, transactionShape);
  }

  private static UpdateFormat updateFormat(
      Filter filter,
      Optional<Set<String>> partiesO,
      boolean verbose,
      TransactionShape transactionShape) {

    TransactionFormat transactionFormat =
        transactionFormat(filter, partiesO, verbose, transactionShape);
    return new UpdateFormat(
        Optional.of(transactionFormat),
        Optional.of(transactionFormat.getEventFormat()),
        Optional.empty());
  }

  public TransactionFormat transactionFormat(Optional<Set<String>> parties) {
    return transactionFormat(filter, parties, verbose, transactionShape);
  }

  private static TransactionFormat transactionFormat(
      Filter filter,
      Optional<Set<String>> partiesO,
      boolean verbose,
      TransactionShape transactionShape) {

    EventFormat eventFormat = eventFormat(filter, partiesO, verbose);
    return new TransactionFormat(eventFormat, transactionShape);
  }

  public EventFormat eventFormat(Optional<Set<String>> parties) {
    return eventFormat(filter, parties, verbose);
  }

  private static EventFormat eventFormat(
      Filter filter, Optional<Set<String>> partiesO, boolean verbose) {
    TransactionFilter transactionFilter = transactionFilter(filter, partiesO);
    return new EventFormat(
        transactionFilter.getPartyToFilters(), transactionFilter.getAnyPartyFilter(), verbose);
  }
}
