// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Identifier;
import com.daml.ledger.javaapi.data.TransactionFilter;

import java.util.Set;

/** The commonality between {@link ContractCompanion} and {@link InterfaceCompanion}. */
public abstract class ContractTypeCompanion<Maker, Data> {
  /** The full template ID of the template or interface that defined this companion. */
  public final Identifier TEMPLATE_ID;

  protected ContractTypeCompanion(Identifier templateId) {
    TEMPLATE_ID = templateId;
  }

  public abstract TransactionFilter transactionFilter(Set<String> parties);
}
