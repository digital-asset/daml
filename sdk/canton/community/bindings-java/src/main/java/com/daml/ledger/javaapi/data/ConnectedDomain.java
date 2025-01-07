// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class ConnectedDomain {
  private final @NonNull String synchronizerAlias;
  private final @NonNull String synchronizerId;

  private final @NonNull ParticipantPermission permission;

  public ConnectedDomain(
      @NonNull String synchronizerAlias,
      @NonNull String synchronizerId,
      @NonNull ParticipantPermission permission) {
    this.synchronizerAlias = synchronizerAlias;
    this.synchronizerId = synchronizerId;
    this.permission = permission;
  }

  @NonNull
  String getSynchronizerAlias() {
    return synchronizerAlias;
  }

  @NonNull
  String getSynchronizerId() {
    return synchronizerId;
  }

  @NonNull
  ParticipantPermission getPermission() {
    return permission;
  }

  public static ConnectedDomain fromProto(
      StateServiceOuterClass.GetConnectedDomainsResponse.ConnectedDomain domain) {
    return new ConnectedDomain(
        domain.getSynchronizerAlias(),
        domain.getSynchronizerId(),
        ParticipantPermission.fromProto(domain.getPermission()));
  }

  public StateServiceOuterClass.GetConnectedDomainsResponse.ConnectedDomain toProto() {
    return StateServiceOuterClass.GetConnectedDomainsResponse.ConnectedDomain.newBuilder()
        .setSynchronizerAlias(synchronizerAlias)
        .setSynchronizerId(synchronizerId)
        .setPermission(permission.toProto())
        .build();
  }

  @Override
  public String toString() {
    return "ConnectedDomain{"
        + "synchronizerAlias="
        + synchronizerAlias
        + ", synchronizerId='"
        + synchronizerId
        + '\''
        + ", permission="
        + permission
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConnectedDomain that = (ConnectedDomain) o;
    return Objects.equals(synchronizerAlias, that.synchronizerAlias)
        && Objects.equals(synchronizerId, that.synchronizerId)
        && Objects.equals(permission, that.permission);
  }

  @Override
  public int hashCode() {
    return Objects.hash(synchronizerAlias, synchronizerId, permission);
  }
}
