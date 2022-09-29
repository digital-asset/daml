// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Identifier;

import java.util.List;

/** The commonality between {@link ContractCompanion} and {@link InterfaceCompanion}. */
public abstract class ContractTypeCompanion<Marker> {
  /** The full template ID of the template or interface that defined this companion. */
  public final Identifier TEMPLATE_ID;
  //TODO: Need to figure out the right type params here
  private final List<ChoiceMetadata<Marker, ?, ?>> choices;

  protected ContractTypeCompanion(Identifier templateId, List<ChoiceMetadata<Marker, ?, ?>> choices) {
    TEMPLATE_ID = templateId;
    this.choices = choices;
  }
}
