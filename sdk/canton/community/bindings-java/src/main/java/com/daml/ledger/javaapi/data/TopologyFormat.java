// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TransactionFilterOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

public final class TopologyFormat {

  @NonNull
  private final Optional<@NonNull ParticipantAuthorizationTopologyFormat>
      includeParticipantAuthorizationEvents;

  public static TopologyFormat fromProto(TransactionFilterOuterClass.TopologyFormat protoFormat) {
    return new TopologyFormat(
        protoFormat.hasIncludeParticipantAuthorizationEvents()
            ? Optional.of(
                ParticipantAuthorizationTopologyFormat.fromProto(
                    protoFormat.getIncludeParticipantAuthorizationEvents()))
            : Optional.empty());
  }

  public TransactionFilterOuterClass.TopologyFormat toProto() {
    return includeParticipantAuthorizationEvents
        .map(
            participantAuthorizationTopologyFormat ->
                TransactionFilterOuterClass.TopologyFormat.newBuilder()
                    .setIncludeParticipantAuthorizationEvents(
                        participantAuthorizationTopologyFormat.toProto())
                    .build())
        .orElseGet(TransactionFilterOuterClass.TopologyFormat::getDefaultInstance);
  }

  public Optional<@NonNull ParticipantAuthorizationTopologyFormat>
      getIncludeParticipantAuthorizationEvents() {
    return includeParticipantAuthorizationEvents;
  }

  public TopologyFormat(
      @NonNull
          Optional<@NonNull ParticipantAuthorizationTopologyFormat>
              includeParticipantAuthorizationEvents) {
    this.includeParticipantAuthorizationEvents = includeParticipantAuthorizationEvents;
  }

  @Override
  public String toString() {
    return "TopologyFormat{includeParticipantAuthorizationEvents="
        + includeParticipantAuthorizationEvents
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TopologyFormat that = (TopologyFormat) o;
    return Objects.equals(
        includeParticipantAuthorizationEvents, that.includeParticipantAuthorizationEvents);
  }

  @Override
  public int hashCode() {
    return Objects.hash(includeParticipantAuthorizationEvents);
  }
}
