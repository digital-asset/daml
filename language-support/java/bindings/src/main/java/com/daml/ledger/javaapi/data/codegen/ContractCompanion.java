// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.CreatedEvent;
import com.daml.ledger.javaapi.data.DamlRecord;
import com.daml.ledger.javaapi.data.Identifier;
import com.daml.ledger.javaapi.data.Value;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public abstract class ContractCompanion<Ct, Id, Data> {
  public final Identifier TEMPLATE_ID;
  final String templateClassName; // not something we want outside this package

  protected final Function<String, Id> newContractId;
  protected final Function<DamlRecord, Data> fromValue;

  public abstract Ct fromCreatedEvent(CreatedEvent event);

  protected ContractCompanion(
      String templateClassName,
      Identifier templateId,
      Function<String, Id> newContractId,
      Function<DamlRecord, Data> fromValue) {
    this.TEMPLATE_ID = templateId;
    this.templateClassName = templateClassName;
    this.newContractId = newContractId;
    this.fromValue = fromValue;
  }

  public static final class WithoutKey<Ct, Id, Data> extends ContractCompanion<Ct, Id, Data> {
    private final NewContract<Ct, Id, Data> newContract;

    public WithoutKey(
        String templateClassName,
        Identifier templateId,
        Function<String, Id> newContractId,
        Function<DamlRecord, Data> fromValue,
        NewContract<Ct, Id, Data> newContract) {
      super(templateClassName, templateId, newContractId, fromValue);
      this.newContract = newContract;
    }

    public Ct fromIdAndRecord(
        String contractId,
        DamlRecord record$,
        Optional<String> agreementText,
        Set<String> signatories,
        Set<String> observers) {
      Id id = newContractId.apply(contractId);
      Data data = fromValue.apply(record$);
      return newContract.newContract(id, data, agreementText, signatories, observers);
    }

    /**
     * @deprecated since introduction; only exists to support generated method that has itself been
     *     deprecated since v0.12.18
     */
    @Deprecated
    public Ct fromIdAndRecord(String contractId, DamlRecord record$) {
      return fromIdAndRecord(
          contractId, record$, Optional.empty(), Collections.emptySet(), Collections.emptySet());
    }

    @Override
    public Ct fromCreatedEvent(CreatedEvent event) {
      return fromIdAndRecord(
          event.getContractId(),
          event.getArguments(),
          event.getAgreementText(),
          event.getSignatories(),
          event.getObservers());
    }

    @FunctionalInterface
    public interface NewContract<Ct, Id, Data> {
      Ct newContract(
          Id id,
          Data data,
          Optional<String> agreementText,
          Set<String> signatories,
          Set<String> observers);
    }
  }

  public static final class WithKey<Ct, Id, Data, Key> extends ContractCompanion<Ct, Id, Data> {
    private final NewContract<Ct, Id, Data, Key> newContract;
    private final Function<Value, Key> keyFromValue;

    public WithKey(
        String templateClassName,
        Identifier templateId,
        Function<String, Id> newContractId,
        Function<DamlRecord, Data> fromValue,
        NewContract<Ct, Id, Data, Key> newContract,
        Function<Value, Key> keyFromValue) {
      super(templateClassName, templateId, newContractId, fromValue);
      this.newContract = newContract;
      this.keyFromValue = keyFromValue;
    }

    public Ct fromIdAndRecord(
        String contractId,
        DamlRecord record$,
        Optional<String> agreementText,
        Optional<Key> key,
        Set<String> signatories,
        Set<String> observers) {
      Id id = newContractId.apply(contractId);
      Data data = fromValue.apply(record$);
      return newContract.newContract(id, data, agreementText, key, signatories, observers);
    }

    /**
     * @deprecated since introduction; only exists to support generated method that has itself been
     *     deprecated since v0.12.18
     */
    @Deprecated
    public Ct fromIdAndRecord(String contractId, DamlRecord record$) {
      return fromIdAndRecord(
          contractId,
          record$,
          Optional.empty(),
          Optional.empty(),
          Collections.emptySet(),
          Collections.emptySet());
    }

    @Override
    public Ct fromCreatedEvent(CreatedEvent event) {
      return fromIdAndRecord(
          event.getContractId(),
          event.getArguments(),
          event.getAgreementText(),
          event.getContractKey().map(keyFromValue),
          event.getSignatories(),
          event.getObservers());
    }

    @FunctionalInterface
    public interface NewContract<Ct, Id, Data, Key> {
      Ct newContract(
          Id id,
          Data data,
          Optional<String> agreementText,
          Optional<Key> key,
          Set<String> signatories,
          Set<String> observers);
    }
  }
}
