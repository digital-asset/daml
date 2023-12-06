// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.admin.UserManagementServiceOuterClass;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class ListUserRightsResponse {

  private final List<User.Right> rights;

  public ListUserRightsResponse(@NonNull List<User.Right> rights) {
    this.rights = new ArrayList<>(rights);
  }

  public List<User.Right> getRights() {
    return new ArrayList<>(this.rights);
  }

  public static ListUserRightsResponse fromProto(
      UserManagementServiceOuterClass.ListUserRightsResponse proto) {
    return new ListUserRightsResponse(
        proto.getRightsList().stream().map(User.Right::fromProto).collect(Collectors.toList()));
  }

  @Override
  public String toString() {
    return "ListUserRightsResponse{" + "rights=" + rights + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ListUserRightsResponse that = (ListUserRightsResponse) o;
    return Objects.equals(rights, that.rights);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rights);
  }
}
