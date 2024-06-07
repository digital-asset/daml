// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.javaapi.data.codegen.Contract;
import com.daml.ledger.javaapi.data.codegen.ContractCompanion;
import com.daml.ledger.javaapi.data.codegen.ContractTypeCompanion;
import com.daml.ledger.javaapi.data.codegen.InterfaceCompanion;
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

  private final Filter filter;

  private ContractFilter(ContractTypeCompanion<Ct, ?, ?, ?> companion, Filter filter) {
    this.companion = companion;
    this.filter = filter;
  }

  public static <Ct> ContractFilter<Ct> of(ContractCompanion<Ct, ?, ?> companion) {
    Filter filter =
        new CumulativeFilter(
            Collections.emptyMap(),
            Collections.singletonMap(
                companion.TEMPLATE_ID, Filter.Template.HIDE_CREATED_EVENT_BLOB),
            Optional.empty());
    return new ContractFilter<>(companion, filter);
  }

  public static <Cid, View> ContractFilter<Contract<Cid, View>> of(
      InterfaceCompanion<?, Cid, View> companion) {
    Filter filter =
        new CumulativeFilter(
            Collections.singletonMap(
                companion.TEMPLATE_ID, Filter.Interface.INCLUDE_VIEW_HIDE_CREATED_EVENT_BLOB),
            Collections.emptyMap(),
            Optional.empty());
    return new ContractFilter<>(companion, filter);
  }

  public Ct toContract(CreatedEvent createdEvent) throws IllegalArgumentException {
    return companion.fromCreatedEvent(createdEvent);
  }

  public TransactionFilter transactionFilter(Set<String> parties) {
    return transactionFilter(filter, parties);
  }

  private static TransactionFilter transactionFilter(Filter filter, Set<String> parties) {
    Map<String, Filter> partyToFilters =
        parties.stream().collect(Collectors.toMap(Function.identity(), x -> filter));
    return new FiltersByParty(partyToFilters);
  }
}
