// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.*;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Metadata and utilities associated with an interface as a whole. Its subclasses serve to
 * disambiguate various generated {@code toInterface} overloads.
 *
 * @param <I> The generated interface marker class.
 */
public abstract class InterfaceCompanion<I, View> extends ContractTypeCompanion<I, View> {

  public final ValueDecoder<View> valueDecoder;

  protected InterfaceCompanion(Identifier templateId, ValueDecoder<View> valueDecoder) {
    super(templateId);
    this.valueDecoder = valueDecoder;
  }

  @Override
  public TransactionFilter transactionFilter(Set<String> parties) {
    Filter filter = new InclusiveFilter(Collections.emptySet(), Map.of(TEMPLATE_ID, Filter.Interface.INCLUDE_VIEW));
    Map<String, Filter> partyToFilters = parties.stream().collect(Collectors.toMap(Function.identity(), x -> filter));
    return new FiltersByParty(partyToFilters);
  }
}
