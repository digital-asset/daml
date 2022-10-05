// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.javaapi.data.DamlRecord;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Metadata and utilities associated with a template as a whole, rather than one single contract
 * made from that template.
 *
 * @param <Ct> The {@link Contract} subclass generated within the template class.
 * @param <Id> The {@link ContractId} subclass generated within the template class.
 * @param <Data> The generated {@link com.daml.ledger.javaapi.data.Template} subclass named after
 *     the template, whose instances contain only the payload.
 */
public abstract class ContractCompanion<Ct, Id, Data> extends ContractTypeCompanion<Data, Data> {
  final String templateClassName; // not something we want outside this package

  protected final Function<String, Id> newContractId;
  protected final Function<DamlRecord, Data> fromValue;

  /**
   * Static method to generate an implementation of {@code ValueDecoder} of type {@code Data} with
   * metadata from the provided {@code ContractCompanion}.
   *
   * @param companion an instance of {@code ContractCompanion}.
   * @return The {@code ValueDecoder} for parsing {@code Value} to get an instance of {@code Data}.
   */
  public static <Data> ValueDecoder<Data> valueDecoder(
      ContractCompanion<?, ? extends ContractId<Data>, Data> companion) {
    return new ValueDecoder<>() {
      @Override
      public Data decode(Value value) {
        DamlRecord record =
            value
                .asRecord()
                .orElseThrow(
                    () ->
                        new IllegalArgumentException("Contracts must be constructed from Records"));
        return companion.fromValue.apply(record);
      }

      @Override
      public ContractId<Data> fromContractId(String contractId) {
        return companion.newContractId.apply(contractId);
      }
    };
  }

  /**
   * Tries to parse a contract from an event expected to create a {@code Ct} contract.
   *
   * @param event the event to try to parse a contract from
   * @throws IllegalArgumentException when the {@link CreatedEvent#arguments} cannot be parsed as
   *     {@code Data}, or the {@link CreatedEvent#contractKey} cannot be parsed as {@code Key}.
   * @return The parsed contract, with payload and metadata, if present.
   */
  public abstract Ct fromCreatedEvent(CreatedEvent event);

  public Id toContractId(ContractId<Data> parameterizedContractId) {
    return newContractId.apply(parameterizedContractId.contractId);
  }

  protected ContractCompanion(
      String templateClassName,
      Identifier templateId,
      Function<String, Id> newContractId,
      Function<DamlRecord, Data> fromValue) {
    super(templateId);
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

  /** @param <Key> {@code Data}'s key type as represented in Java codegen. */
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
