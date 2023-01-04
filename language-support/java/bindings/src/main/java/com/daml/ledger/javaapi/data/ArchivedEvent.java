// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.EventOuterClass;
import java.util.List;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class ArchivedEvent implements Event {

  private final List<String> witnessParties;

  private final String eventId;

  private final Identifier templateId;

  private final String contractId;

  public ArchivedEvent(
      @NonNull List<@NonNull String> witnessParties,
      @NonNull String eventId,
      @NonNull Identifier templateId,
      @NonNull String contractId) {
    this.witnessParties = witnessParties;
    this.eventId = eventId;
    this.templateId = templateId;
    this.contractId = contractId;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ArchivedEvent that = (ArchivedEvent) o;
    return Objects.equals(witnessParties, that.witnessParties)
        && Objects.equals(eventId, that.eventId)
        && Objects.equals(templateId, that.templateId)
        && Objects.equals(contractId, that.contractId);
  }

  @Override
  public int hashCode() {

    return Objects.hash(witnessParties, eventId, templateId, contractId);
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
        + '}';
  }

  public EventOuterClass.ArchivedEvent toProto() {
    return EventOuterClass.ArchivedEvent.newBuilder()
        .setContractId(getContractId())
        .setEventId(getEventId())
        .setTemplateId(getTemplateId().toProto())
        .addAllWitnessParties(this.getWitnessParties())
        .build();
  }

  public static ArchivedEvent fromProto(EventOuterClass.ArchivedEvent archivedEvent) {
    return new ArchivedEvent(
        archivedEvent.getWitnessPartiesList(),
        archivedEvent.getEventId(),
        Identifier.fromProto(archivedEvent.getTemplateId()),
        archivedEvent.getContractId());
  }
}
