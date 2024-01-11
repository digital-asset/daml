// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.admin.UserManagementServiceOuterClass;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class RevokeUserRightsResponse {

  private final List<User.Right> newlyRevokedRights;

  public RevokeUserRightsResponse(@NonNull List<User.Right> newlyRevokedRights) {
    this.newlyRevokedRights = new ArrayList<>(newlyRevokedRights);
  }

  public List<User.Right> getNewlyRevokedRights() {
    return new ArrayList<>(this.newlyRevokedRights);
  }

  public static RevokeUserRightsResponse fromProto(
      UserManagementServiceOuterClass.RevokeUserRightsResponse proto) {
    return new RevokeUserRightsResponse(
        proto.getNewlyRevokedRightsList().stream()
            .map(User.Right::fromProto)
            .collect(Collectors.toList()));
  }

  @Override
  public String toString() {
    return "RevokeUserRightsResponse{" + "newlyRevokedRights=" + newlyRevokedRights + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RevokeUserRightsResponse that = (RevokeUserRightsResponse) o;
    return Objects.equals(newlyRevokedRights, that.newlyRevokedRights);
  }

  @Override
  public int hashCode() {
    return Objects.hash(newlyRevokedRights);
  }
}
