// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.EventOuterClass;
import com.google.protobuf.StringValue;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class ArchivedEvent implements Event {

  private final List<String> witnessParties;

  private final String eventId;

  private final Identifier templateId;

  private final String contractId;

  private final Optional<String> packageName;

  public ArchivedEvent(
      @NonNull List<@NonNull String> witnessParties,
      @NonNull String eventId,
      @NonNull Identifier templateId,
      @NonNull String contractId,
      @NonNull Optional<String> packageName) {
    this.witnessParties = witnessParties;
    this.eventId = eventId;
    this.templateId = templateId;
    this.contractId = contractId;
    this.packageName = packageName;
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
  public Optional<String> getPackageName() {
    return packageName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ArchivedEvent that = (ArchivedEvent) o;
    return Objects.equals(witnessParties, that.witnessParties)
        && Objects.equals(eventId, that.eventId)
        && Objects.equals(templateId, that.templateId)
        && Objects.equals(contractId, that.contractId)
        && Objects.equals(packageName, that.packageName);
  }

  @Override
  public int hashCode() {

    return Objects.hash(witnessParties, eventId, templateId, contractId, packageName);
  }

  @Override
  public String toString() {
    return "ArchivedEvent{"
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
        + ", packageName="
        + packageName
        + '}';
  }

  public EventOuterClass.ArchivedEvent toProto() {
    EventOuterClass.ArchivedEvent.Builder builder = EventOuterClass.ArchivedEvent.newBuilder()
        .setContractId(getContractId())
        .setEventId(getEventId())
        .setTemplateId(getTemplateId().toProto())
        .addAllWitnessParties(this.getWitnessParties());
    packageName.ifPresent(a -> builder.setPackageName(StringValue.of(a)));
    return builder.build();
  }

  public static ArchivedEvent fromProto(EventOuterClass.ArchivedEvent archivedEvent) {
    return new ArchivedEvent(
        archivedEvent.getWitnessPartiesList(),
        archivedEvent.getEventId(),
        Identifier.fromProto(archivedEvent.getTemplateId()),
        archivedEvent.getContractId(),
        archivedEvent.hasPackageName() ? Optional.of(archivedEvent.getPackageName().getValue()) : Optional.empty()
    );
  }
}
