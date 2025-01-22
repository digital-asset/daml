// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TopologyTransactionOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.*;

public final class ParticipantAuthorizationChanged implements TopologyEvent {

  private final @NonNull String partyId;

  private final @NonNull String participantId;

  private final @NonNull ParticipantPermission permission;

  public ParticipantAuthorizationChanged(
      @NonNull String partyId,
      @NonNull String participantId,
      @NonNull ParticipantPermission permission) {
    this.partyId = partyId;
    this.participantId = participantId;
    this.permission = permission;
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

  @NonNull
  public ParticipantPermission getPermission() {
    return permission;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ParticipantAuthorizationChanged that = (ParticipantAuthorizationChanged) o;
    return Objects.equals(partyId, that.partyId)
        && Objects.equals(participantId, that.participantId)
        && Objects.equals(permission, that.permission);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partyId, participantId, permission);
  }

  @Override
  public String toString() {
    return "CreatedEvent{"
        + "partyId="
        + partyId
        + ", participantId='"
        + participantId
        + ", permission="
        + permission
        + '}';
  }

  public TopologyTransactionOuterClass.@NonNull ParticipantAuthorizationChanged toProto() {
    return TopologyTransactionOuterClass.ParticipantAuthorizationChanged.newBuilder()
        .setPartyId(this.getPartyId())
        .setParticipantId(this.getParticipantId())
        .setParticipantPermission(this.getPermission().toProto())
        .build();
  }

  public static ParticipantAuthorizationChanged fromProto(
      TopologyTransactionOuterClass.ParticipantAuthorizationChanged changedEvent) {
    return new ParticipantAuthorizationChanged(
        changedEvent.getPartyId(),
        changedEvent.getParticipantId(),
        ParticipantPermission.fromProto(changedEvent.getParticipantPermission()));
  }
}
