// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Identifier;

import java.util.List;

/**
 * Metadata and utilities associated with an interface as a whole. Its subclasses serve to
 * disambiguate various generated {@code toInterface} overloads.
 *
 * @param <I> The generated interface marker class.
 * @param <View> The {@link DamlRecord} subclass representing the interface view, as may be
 *     retrieved from the ACS or transaction stream.
 */
public abstract class InterfaceCompanion<I, View> extends ContractTypeCompanion<I, View> {

  public final ValueDecoder<View> valueDecoder;

  /**
   * <strong>INTERNAL API</strong>: this is meant for use by <a
   * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
   * and <em>should not be referenced directly</em>. Applications should refer to the {@code
   * INTERFACE} field on generated code for Daml interfaces instead.
   */
  protected InterfaceCompanion(Identifier templateId, ValueDecoder<View> valueDecoder, List<ChoiceMetadata<?, ?, ?>> choices) {
    super(templateId, choices);
    this.valueDecoder = valueDecoder;
  }
}
