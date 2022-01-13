// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.admin.UserManagementServiceOuterClass;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class CreateUserRequest {

  private final User user;
  private final List<User.Right> rights;

  public CreateUserRequest(User user, User.Right right, User.Right... rights) {
    this.user = user;
    this.rights = new ArrayList<>(rights.length + 1);
    this.rights.add(right);
    this.rights.addAll(Arrays.asList(rights));
  }

  public CreateUserRequest(@NonNull String id, @NonNull String primaryParty) {
    this(new User(id, primaryParty), new User.Right.CanActAs(primaryParty));
  }

  public User getUser() {
    return user;
  }

  public List<User.Right> getRights() {
    return new ArrayList<>(rights);
  }

  public UserManagementServiceOuterClass.CreateUserRequest toProto() {
    return UserManagementServiceOuterClass.CreateUserRequest.newBuilder()
        .setUser(this.user.toProto())
        .addAllRights(this.rights.stream().map(User.Right::toProto).collect(Collectors.toList()))
        .build();
  }

  @Override
  public String toString() {
    return "CreateUserRequest{" + "user=" + user + ", rights=" + rights + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateUserRequest that = (CreateUserRequest) o;
    return user.equals(that.user) && rights.equals(that.rights);
  }

  @Override
  public int hashCode() {
    return Objects.hash(user, rights);
  }
}
