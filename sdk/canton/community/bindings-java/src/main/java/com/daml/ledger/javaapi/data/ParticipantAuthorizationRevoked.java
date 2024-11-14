// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TopologyTransactionOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class ParticipantAuthorizationRevoked implements TopologyEvent {

  private final @NonNull String partyId;

  private final @NonNull String participantId;

  public ParticipantAuthorizationRevoked(@NonNull String partyId, @NonNull String participantId) {
    this.partyId = partyId;
    this.participantId = participantId;
  }

  @NonNull
  public String getPartyId() {
    return partyId;
  }

  @NonNull
  @Override
  public String getParticipantId() {
    return participantId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ParticipantAuthorizationRevoked that = (ParticipantAuthorizationRevoked) o;
    return Objects.equals(partyId, that.partyId)
        && Objects.equals(participantId, that.participantId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partyId, participantId);
  }

  @Override
  public String toString() {
    return "CreatedEvent{" + "partyId=" + partyId + ", participantId='" + participantId + '}';
  }

  public TopologyTransactionOuterClass.@NonNull ParticipantAuthorizationRevoked toProto() {
    return TopologyTransactionOuterClass.ParticipantAuthorizationRevoked.newBuilder()
        .setPartyId(this.getPartyId())
        .setParticipantId(this.getParticipantId())
        .build();
  }

  public static ParticipantAuthorizationRevoked fromProto(
      TopologyTransactionOuterClass.ParticipantAuthorizationRevoked changedEvent) {
    return new ParticipantAuthorizationRevoked(
        changedEvent.getPartyId(), changedEvent.getParticipantId());
  }
}
