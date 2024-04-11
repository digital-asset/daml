// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class ConnectedDomain {
  private final @NonNull String domainAlias;
  private final @NonNull String domainId;

  private final @NonNull ParticipantPermission permission;

  public ConnectedDomain(
      @NonNull String domainAlias,
      @NonNull String domainId,
      @NonNull ParticipantPermission permission) {
    this.domainAlias = domainAlias;
    this.domainId = domainId;
    this.permission = permission;
  }

  @NonNull
  String getDomainAlias() {
    return domainAlias;
  }

  @NonNull
  String getDomainId() {
    return domainId;
  }

  @NonNull
  ParticipantPermission getPermission() {
    return permission;
  }

  public static ConnectedDomain fromProto(
      StateServiceOuterClass.GetConnectedDomainsResponse.ConnectedDomain domain) {
    return new ConnectedDomain(
        domain.getDomainAlias(),
        domain.getDomainId(),
        ParticipantPermission.fromProto(domain.getPermission()));
  }

  public StateServiceOuterClass.GetConnectedDomainsResponse.ConnectedDomain toProto() {
    return StateServiceOuterClass.GetConnectedDomainsResponse.ConnectedDomain.newBuilder()
        .setDomainAlias(domainAlias)
        .setDomainId(domainId)
        .setPermission(permission.toProto())
        .build();
  }

  @Override
  public String toString() {
    return "ConnectedDomain{"
        + "domainAlias="
        + domainAlias
        + ", domainId='"
        + domainId
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
        && Objects.equals(domainId, that.domainId)
        && Objects.equals(permission, that.permission);
  }

  @Override
  public int hashCode() {
    return Objects.hash(domainAlias, domainId, permission);
  }
}
