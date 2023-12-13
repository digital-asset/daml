// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class ConnectedDomainV2 {
  private final @NonNull String domainAlias;
  private final @NonNull String domainId;

  private final @NonNull ParticipantPermissionV2 permission;

  public ConnectedDomainV2(
      @NonNull String domainAlias,
      @NonNull String domainId,
      @NonNull ParticipantPermissionV2 permission) {
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
  ParticipantPermissionV2 getPermission() {
    return permission;
  }

  public static ConnectedDomainV2 fromProto(
      StateServiceOuterClass.GetConnectedDomainsResponse.ConnectedDomain domain) {
    return new ConnectedDomainV2(
        domain.getDomainAlias(),
        domain.getDomainId(),
        ParticipantPermissionV2.fromProto(domain.getPermission()));
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
    ConnectedDomainV2 that = (ConnectedDomainV2) o;
    return Objects.equals(domainAlias, that.domainAlias)
        && Objects.equals(domainId, that.domainId)
        && Objects.equals(permission, that.permission);
  }

  @Override
  public int hashCode() {
    return Objects.hash(domainAlias, domainId, permission);
  }
}
