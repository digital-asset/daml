// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.javaapi.data.DamlRecord;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Metadata and utilities associated with an interface as a whole. Its subclasses serve to
 * disambiguate various generated {@code toInterface} overloads.
 *
 * @param <I> The generated interface marker class.
 * @param <View> The {@link DamlRecord} subclass representing the interface view, as may be
 *     retrieved from the ACS or transaction stream.
 */
public abstract class InterfaceCompanion<I, Id, View> extends ContractTypeCompanion<I, View> {

  protected final Function<String, Id> newContractId;

  public final ValueDecoder<View> valueDecoder;

  private Contract<Id, View> fromIdAndRecord(
      String contractId,
      Map<Identifier, DamlRecord> interfaceViews,
      Optional<String> agreementText,
      Set<String> signatories,
      Set<String> observers)
      throws IllegalAccessException {
    Optional<DamlRecord> maybeRecord = Optional.ofNullable(interfaceViews.get(TEMPLATE_ID));
    Id id = newContractId.apply(contractId);
    if (maybeRecord.isPresent()) {
      View view = valueDecoder.decode(maybeRecord.get());
      return new ContractWithInterfaceView<>(id, view, agreementText, signatories, observers);
    } else {
      // TODO: CL handle all exceptional cases
      throw new IllegalAccessException("interface view not found. It could be failed");
    }
  }

  public final Contract<Id, View> fromCreatedEvent(CreatedEvent event)
      throws IllegalAccessException {
    return fromIdAndRecord(
        event.getContractId(),
        event.getInterfaceViews(),
        event.getAgreementText(),
        event.getSignatories(),
        event.getObservers());
  }

  /**
   * <strong>INTERNAL API</strong>: this is meant for use by <a
   * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
   * and <em>should not be referenced directly</em>. Applications should refer to the {@code
   * INTERFACE} field on generated code for Daml interfaces instead.
   */
  protected InterfaceCompanion(
      Identifier templateId, Function<String, Id> newContractId, ValueDecoder<View> valueDecoder) {
    super(templateId);
    this.newContractId = newContractId;
    this.valueDecoder = valueDecoder;
  }
}
