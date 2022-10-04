// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Identifier;

/** The commonality between {@link ContractCompanion} and {@link InterfaceCompanion}. */
public abstract class ContractTypeCompanion<Maker, Data> {
  /** The full template ID of the template or interface that defined this companion. */
  public final Identifier TEMPLATE_ID;

  /**
   * <strong>INTERNAL API</strong>: this is meant for use by {@link ContractCompanion} and {@link
   * InterfaceCompanion}, and <em>should not be referenced directly</em>. Applications should refer
   * to code-generated {@code COMPANION} and {@code INTERFACE} fields specific to the template or
   * interface in question instead.
   */
  protected ContractTypeCompanion(Identifier templateId) {
    TEMPLATE_ID = templateId;
  }
}
