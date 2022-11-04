// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.javaapi.data.codegen.Contract;
import com.daml.ledger.javaapi.data.codegen.ContractCompanion;
import com.daml.ledger.javaapi.data.codegen.InterfaceCompanion;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class contains utilities to decode a <code>CreatedEvent</code> and create a <code>
 * TransactionFilter</code> by provider parties It can only be instantiated with a subtype of <code>
 * ContractCompanion</code>
 */
public final class ContractFilter<Ct> {
  private final FromCreatedEventFunc<CreatedEvent, Ct> fromCreatedEvent;

  private final Filter filter;

  private ContractFilter(FromCreatedEventFunc<CreatedEvent, Ct> fromCreatedEvent, Filter filter) {
    this.fromCreatedEvent = fromCreatedEvent;
    this.filter = filter;
  }

  public static <Ct> ContractFilter<Ct> of(ContractCompanion<Ct, ?, ?> companion) {
    Filter filter =
        new InclusiveFilter(Collections.singleton(companion.TEMPLATE_ID), Collections.emptyMap());
    return new ContractFilter<>(companion::fromCreatedEvent, filter);
  }

  public static <Cid, View> ContractFilter<Contract<Cid, View>> of(
      InterfaceCompanion<?, Cid, View> companion) {
    Filter filter =
        new InclusiveFilter(
            Collections.emptySet(),
            Collections.singletonMap(companion.TEMPLATE_ID, Filter.Interface.INCLUDE_VIEW));
    return new ContractFilter<>(companion::fromCreatedEvent, filter);
  }

  public Ct toContract(CreatedEvent createdEvent) throws IllegalArgumentException {
    return fromCreatedEvent.apply(createdEvent);
  }

  public TransactionFilter transactionFilter(Set<String> parties) {
    return transactionFilter(filter, parties);
  }

  private static TransactionFilter transactionFilter(Filter filter, Set<String> parties) {
    Map<String, Filter> partyToFilters =
        parties.stream().collect(Collectors.toMap(Function.identity(), x -> filter));
    return new FiltersByParty(partyToFilters);
  }

  @FunctionalInterface
  private interface FromCreatedEventFunc<T, R> {
    R apply(T t) throws IllegalArgumentException;
  }
}
