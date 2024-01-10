// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.CreatedEvent;
import com.daml.ledger.javaapi.data.DamlRecord;
import com.daml.ledger.javaapi.data.Identifier;
import com.daml.ledger.javaapi.data.Value;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoder;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Metadata and utilities associated with a template as a whole, rather than one single contract
 * made from that template.
 *
 * <p>Application code <em>should not</em> instantiate or subclass; instead, refer to the {@code
 * COMPANION} field on generated {@link com.daml.ledger.javaapi.data.Template} subclasses. All
 * {@code protected} members herein are considered part of the <strong>INTERNAL API</strong>.
 *
 * <p>Every instance is either a {@link WithKey} or {@link WithoutKey}, depending on whether the
 * template defined a {@code key} type. {@link WithKey} defines extra utilities for working with
 * contract keys.
 *
 * @param <Ct> The {@link Contract} subclass generated within the template class.
 * @param <Id> The {@link ContractId} subclass generated within the template class.
 * @param <Data> The generated {@link com.daml.ledger.javaapi.data.Template} subclass named after
 *     the template, whose instances contain only the payload.
 */
public abstract class ContractCompanion<Ct, Id, Data>
    extends ContractTypeCompanion<Ct, Id, Data, Data> {
  /** @hidden */
  protected final Function<DamlRecord, Data> fromValue;

  @FunctionalInterface // Defines the function type which throws.
  public static interface FromJson<T> {
    T decode(String s) throws JsonLfDecoder.Error;
  }

  protected final FromJson<Data> fromJson;

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
   * <strong>INTERNAL API</strong>: this is meant for use by {@link WithoutKey} and {@link WithKey},
   * and <em>should not be referenced directly</em>. Applications should refer to the {@code
   * COMPANION} field on generated {@link com.daml.ledger.javaapi.data.Template} subclasses instead.
   *
   * @hidden
   */
  protected ContractCompanion(
      String templateClassName,
      Identifier templateId,
      Function<String, Id> newContractId,
      Function<DamlRecord, Data> fromValue,
      FromJson<Data> fromJson,
      List<Choice<Data, ?, ?>> choices) {
    super(templateId, templateClassName, newContractId, choices);
    this.fromValue = fromValue;
    this.fromJson = fromJson;
  }

  public Data fromJson(String json) throws JsonLfDecoder.Error {
    return this.fromJson.decode(json);
  }

  public static final class WithoutKey<Ct, Id, Data> extends ContractCompanion<Ct, Id, Data> {
    private final NewContract<Ct, Id, Data> newContract;

    /**
     * <strong>INTERNAL API</strong>: this is meant for use by <a
     * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
     * and <em>should not be referenced directly</em>. Applications should refer to the {@code
     * COMPANION} field on generated {@link com.daml.ledger.javaapi.data.Template} subclasses
     * instead.
     *
     * @hidden
     */
    public WithoutKey(
        String templateClassName,
        Identifier templateId,
        Function<String, Id> newContractId,
        Function<DamlRecord, Data> fromValue,
        FromJson<Data> fromJson,
        NewContract<Ct, Id, Data> newContract,
        List<Choice<Data, ?, ?>> choices) {
      super(templateClassName, templateId, newContractId, fromValue, fromJson, choices);
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

    /**
     * <strong>INTERNAL API</strong>: this is meant for use by <a
     * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
     * and <em>should not be referenced directly</em>. Applications should refer to the {@code
     * COMPANION} field on generated {@link com.daml.ledger.javaapi.data.Template} subclasses
     * instead.
     *
     * @hidden
     */
    public WithKey(
        String templateClassName,
        Identifier templateId,
        Function<String, Id> newContractId,
        Function<DamlRecord, Data> fromValue,
        FromJson<Data> fromJson,
        NewContract<Ct, Id, Data, Key> newContract,
        List<Choice<Data, ?, ?>> choices,
        Function<Value, Key> keyFromValue) {
      super(templateClassName, templateId, newContractId, fromValue, fromJson, choices);
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
