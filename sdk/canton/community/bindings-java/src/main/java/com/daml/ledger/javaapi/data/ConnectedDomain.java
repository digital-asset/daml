// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class ConnectedDomain {
  private final @NonNull String domainAlias;
  private final @NonNull String synchronizerId;

  private final @NonNull ParticipantPermission permission;

  public ConnectedDomain(
      @NonNull String domainAlias,
      @NonNull String synchronizerId,
      @NonNull ParticipantPermission permission) {
    this.domainAlias = domainAlias;
    this.synchronizerId = synchronizerId;
    this.permission = permission;
  }

  @NonNull
  String getDomainAlias() {
    return domainAlias;
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
        domain.getDomainAlias(),
        domain.getSynchronizerId(),
        ParticipantPermission.fromProto(domain.getPermission()));
  }

  public StateServiceOuterClass.GetConnectedDomainsResponse.ConnectedDomain toProto() {
    return StateServiceOuterClass.GetConnectedDomainsResponse.ConnectedDomain.newBuilder()
        .setDomainAlias(domainAlias)
        .setSynchronizerId(synchronizerId)
        .setPermission(permission.toProto())
        .build();
  }

  @Override
  public String toString() {
    return "ConnectedDomain{"
        + "domainAlias="
        + domainAlias
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
    return Objects.equals(domainAlias, that.domainAlias)
        && Objects.equals(synchronizerId, that.synchronizerId)
        && Objects.equals(permission, that.permission);
  }

  @Override
  public int hashCode() {
    return Objects.hash(domainAlias, synchronizerId, permission);
  }
}
