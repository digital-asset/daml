// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Identifier;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The commonality between {@link ContractCompanion} and {@link InterfaceCompanion}.
 *
 * @param <ContractType> The type argument to {@link ContractId}s of this contract type. This is the
 *     same as {@code Data} for templates, but is a pure marker type for interfaces.
 * @param <Data> The "payload" data model for a contract. This is the template payload for
 *     templates, and the view type for interfaces.
 */
public abstract class ContractTypeCompanion<ContractType, Data> {
  /** The full template ID of the template or interface that defined this companion. */
  public final Identifier TEMPLATE_ID;

  final String TEMPLATE_CLASS_NAME;

  /**
   * The provides a mapping of choice name to Choice.
   *
   * <pre>
   * // if you statically know the name of a choice
   * var c1 = Bar.COMPANION.choices.get("Transfer");
   * // it is better to retrieve it directly from the generated field
   * var c2 = Bar.CHOICE_Transfer;
   * </pre>
   */
  public final Map<String, Choice<ContractType, ?, ?>> choices;

  /**
   * <strong>INTERNAL API</strong>: this is meant for use by {@link ContractCompanion} and {@link
   * InterfaceCompanion}, and <em>should not be referenced directly</em>. Applications should refer
   * to code-generated {@code COMPANION} and {@code INTERFACE} fields specific to the template or
   * interface in question instead.
   */
  protected ContractTypeCompanion(
      Identifier templateId, String templateClassName, List<Choice<ContractType, ?, ?>> choices) {
    TEMPLATE_ID = templateId;
    TEMPLATE_CLASS_NAME = templateClassName;
    this.choices =
        choices.stream().collect(Collectors.toMap(choice -> choice.name, Function.identity()));
  }
}
