// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.EventOuterClass;
import com.google.protobuf.StringValue;
import java.util.*;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class CreatedEvent implements Event, TreeEvent {

  private final @NonNull List<@NonNull String> witnessParties;

  private final String eventId;

  private final Identifier templateId;

  private final String contractId;

  private final DamlRecord arguments;

  private final Optional<String> agreementText;

  private final Optional<Value> contractKey;

  private final @NonNull Set<@NonNull String> signatories;

  private final @NonNull Set<@NonNull String> observers;

  public CreatedEvent(
      @NonNull List<@NonNull String> witnessParties,
      @NonNull String eventId,
      @NonNull Identifier templateId,
      @NonNull String contractId,
      @NonNull DamlRecord arguments,
      @NonNull Optional<String> agreementText,
      @NonNull Optional<Value> contractKey,
      @NonNull Collection<@NonNull String> signatories,
      @NonNull Collection<@NonNull String> observers) {
    this.witnessParties = Collections.unmodifiableList(new ArrayList<>(witnessParties));
    this.eventId = eventId;
    this.templateId = templateId;
    this.contractId = contractId;
    this.arguments = arguments;
    this.agreementText = agreementText;
    this.contractKey = contractKey;
    this.signatories = Collections.unmodifiableSet(new HashSet<>(signatories));
    this.observers = Collections.unmodifiableSet(new HashSet<>(observers));
  }

  @NonNull
  @Override
  public List<@NonNull String> getWitnessParties() {
    return witnessParties;
  }

  @NonNull
  @Override
  public String getEventId() {
    return eventId;
  }

  @NonNull
  @Override
  public Identifier getTemplateId() {
    return templateId;
  }

  @NonNull
  @Override
  public String getContractId() {
    return contractId;
  }

  @NonNull
  public DamlRecord getArguments() {
    return arguments;
  }

  @NonNull
  public Optional<String> getAgreementText() {
    return agreementText;
  }

  @NonNull
  public Optional<Value> getContractKey() {
    return contractKey;
  }

  @NonNull
  public Set<@NonNull String> getSignatories() {
    return signatories;
  }

  @NonNull
  public Set<@NonNull String> getObservers() {
    return observers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreatedEvent that = (CreatedEvent) o;
    return Objects.equals(witnessParties, that.witnessParties)
        && Objects.equals(eventId, that.eventId)
        && Objects.equals(templateId, that.templateId)
        && Objects.equals(contractId, that.contractId)
        && Objects.equals(arguments, that.arguments)
        && Objects.equals(agreementText, that.agreementText)
        && Objects.equals(contractKey, that.contractKey)
        && Objects.equals(signatories, that.signatories)
        && Objects.equals(observers, that.observers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        witnessParties,
        eventId,
        templateId,
        contractId,
        arguments,
        agreementText,
        contractKey,
        signatories,
        observers);
  }

  @Override
  public String toString() {
    return "CreatedEvent{"
        + "witnessParties="
        + witnessParties
        + ", eventId='"
        + eventId
        + '\''
        + ", templateId="
        + templateId
        + ", contractId='"
        + contractId
        + '\''
        + ", arguments="
        + arguments
        + ", agreementText='"
        + agreementText
        + '\''
        + ", contractKey="
        + contractKey
        + ", signatories="
        + signatories
        + ", observers="
        + observers
        + '}';
  }

  public EventOuterClass.@NonNull CreatedEvent toProto() {
    EventOuterClass.CreatedEvent.Builder builder =
        EventOuterClass.CreatedEvent.newBuilder()
            .setContractId(this.getContractId())
            .setCreateArguments(this.getArguments().toProtoRecord())
            .setEventId(this.getEventId())
            .setTemplateId(this.getTemplateId().toProto())
            .addAllWitnessParties(this.getWitnessParties())
            .addAllSignatories(this.getSignatories())
            .addAllObservers(this.getObservers());
    agreementText.ifPresent(a -> builder.setAgreementText(StringValue.of(a)));
    contractKey.ifPresent(a -> builder.setContractKey(a.toProto()));
    return builder.build();
  }

  public static CreatedEvent fromProto(EventOuterClass.CreatedEvent createdEvent) {
    return new CreatedEvent(
        createdEvent.getWitnessPartiesList(),
        createdEvent.getEventId(),
        Identifier.fromProto(createdEvent.getTemplateId()),
        createdEvent.getContractId(),
        DamlRecord.fromProto(createdEvent.getCreateArguments()),
        createdEvent.hasAgreementText()
            ? Optional.of(createdEvent.getAgreementText().getValue())
            : Optional.empty(),
        createdEvent.hasContractKey()
            ? Optional.of(Value.fromProto(createdEvent.getContractKey()))
            : Optional.empty(),
        createdEvent.getSignatoriesList(),
        createdEvent.getObserversList());
  }
}
