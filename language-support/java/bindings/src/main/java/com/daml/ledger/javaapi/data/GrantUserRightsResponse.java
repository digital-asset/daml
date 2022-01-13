// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.admin.UserManagementServiceOuterClass;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class GrantUserRightsResponse {

  private final List<User.Right> newlyGrantedRights;

  public GrantUserRightsResponse(@NonNull List<User.Right> newlyGrantedRights) {
    this.newlyGrantedRights = new ArrayList<>(newlyGrantedRights);
  }

  public List<User.Right> getNewlyGrantedRights() {
    return new ArrayList<>(this.newlyGrantedRights);
  }

  public static GrantUserRightsResponse fromProto(
      UserManagementServiceOuterClass.GrantUserRightsResponse proto) {
    return new GrantUserRightsResponse(
        proto.getNewlyGrantedRightsList().stream()
            .map(User.Right::fromProto)
            .collect(Collectors.toList()));
  }

  @Override
  public String toString() {
    return "GrantUserRightsResponse{" + "newlyGrantedRights=" + newlyGrantedRights + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GrantUserRightsResponse that = (GrantUserRightsResponse) o;
    return Objects.equals(newlyGrantedRights, that.newlyGrantedRights);
  }

  @Override
  public int hashCode() {
    return Objects.hash(newlyGrantedRights);
  }
}
