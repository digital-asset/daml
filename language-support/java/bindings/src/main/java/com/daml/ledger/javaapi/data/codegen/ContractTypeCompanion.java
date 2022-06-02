// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Identifier;

/** The commonality between {@link ContractCompanion} and {@link InterfaceCompanion}. */
public class ContractTypeCompanion {
  /** The full template ID of the template or interface that defined this companion. */
  public final Identifier TEMPLATE_ID;

  public ContractTypeCompanion(Identifier templateId) {
    TEMPLATE_ID = templateId;
  }
}
