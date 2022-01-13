// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.admin.UserManagementServiceOuterClass;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class ListUsersResponse {

  private final List<User> users;

  public ListUsersResponse(@NonNull List<User> users) {
    this.users = new ArrayList<>(users);
  }

  public List<User> getUsers() {
    return new ArrayList<>(this.users);
  }

  public static ListUsersResponse fromProto(
      UserManagementServiceOuterClass.ListUsersResponse proto) {
    return new ListUsersResponse(
        proto.getUsersList().stream().map(User::fromProto).collect(Collectors.toList()));
  }

  @Override
  public String toString() {
    return "ListUsersResponse{" + "users=" + users + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ListUsersResponse that = (ListUsersResponse) o;
    return Objects.equals(users, that.users);
  }

  @Override
  public int hashCode() {
    return Objects.hash(users);
  }
}
