// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.CreatedEvent;
import com.daml.ledger.javaapi.data.Identifier;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The commonality between {@link ContractCompanion} and {@link InterfaceCompanion}.
 *
 * @param <Ct> The specific type of {@link Contract} representing contracts from the ledger. Always
 *     a subtype of {@code Contract<Id, Data>}.
 * @param <Id> The code-generated class of {@link ContractId}s specific to this template or
 *     interface. Always a subtype of {@code ContractId<ContractType>}.
 * @param <ContractType> The type argument to {@link ContractId}s of this contract type. This is the
 *     same as {@code Data} for templates, but is a pure marker type for interfaces.
 * @param <Data> The "payload" data model for a contract. This is the template payload for
 *     templates, and the view type for interfaces.
 */
public abstract class ContractTypeCompanion<Ct, Id, ContractType, Data> {
  /** The full template ID of the template or interface that defined this companion. */
  public final Identifier TEMPLATE_ID;

  final String TEMPLATE_CLASS_NAME;

  final Function<String, Id> newContractId;

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
   *
   * @hidden
   */
  protected ContractTypeCompanion(
      Identifier templateId,
      String templateClassName,
      Function<String, Id> newContractId,
      List<Choice<ContractType, ?, ?>> choices) {
    TEMPLATE_ID = templateId;
    TEMPLATE_CLASS_NAME = templateClassName;
    this.newContractId = newContractId;
    this.choices =
        choices.stream().collect(Collectors.toMap(choice -> choice.name, Function.identity()));
  }

  /**
   * Tries to parse a contract from an event expected to create a {@code Ct} contract. This is
   * either the {@link CreatedEvent#getArguments} for {@link ContractCompanion}, or one of {@link
   * CreatedEvent#getInterfaceViews} for an {@link InterfaceCompanion}.
   *
   * @param event the event to try to parse a contract from
   * @throws IllegalArgumentException when the {@link CreatedEvent} payload cannot be parsed as
   *     {@code Data}, or the {@link CreatedEvent#getContractKey} cannot be parsed as a contract
   *     key.
   * @return The parsed contract, with payload and metadata, if present.
   */
  public abstract Ct fromCreatedEvent(CreatedEvent event) throws IllegalArgumentException;

  /**
   * TODO
   *
   * @param parameterizedContractId
   * @return
   */
  public final Id toContractId(ContractId<Data> parameterizedContractId) {
    return newContractId.apply(parameterizedContractId.contractId);
  }
}
