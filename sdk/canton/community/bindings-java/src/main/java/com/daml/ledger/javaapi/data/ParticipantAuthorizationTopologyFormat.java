// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TransactionFilterOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public final class ParticipantAuthorizationTopologyFormat {

  private final Set<@NonNull String> parties;

  public static ParticipantAuthorizationTopologyFormat fromProto(
      TransactionFilterOuterClass.ParticipantAuthorizationTopologyFormat protoFormat) {
    return new ParticipantAuthorizationTopologyFormat(new HashSet<>(protoFormat.getPartiesList()));
  }

  public TransactionFilterOuterClass.ParticipantAuthorizationTopologyFormat toProto() {
    return TransactionFilterOuterClass.ParticipantAuthorizationTopologyFormat.newBuilder()
        .addAllParties(parties)
        .build();
  }

  public Set<@NonNull String> getParties() {
    return parties;
  }

  public ParticipantAuthorizationTopologyFormat(@NonNull Set<@NonNull String> parties) {
    this.parties = parties;
  }

  @Override
  public String toString() {
    return "ParticipantAuthorizationTopologyFormat{" + "parties=" + parties + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ParticipantAuthorizationTopologyFormat that = (ParticipantAuthorizationTopologyFormat) o;
    return Objects.equals(parties, that.parties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(parties);
  }
}
