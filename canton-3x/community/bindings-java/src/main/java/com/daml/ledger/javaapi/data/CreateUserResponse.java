// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.admin.UserManagementServiceOuterClass;
import java.util.Objects;

public final class CreateUserResponse {

  private final User user;

  public CreateUserResponse(User user) {
    this.user = user;
  }

  public User getUser() {
    return user;
  }

  public static CreateUserResponse fromProto(
      UserManagementServiceOuterClass.CreateUserResponse proto) {
    return new CreateUserResponse(User.fromProto(proto.getUser()));
  }

  public UserManagementServiceOuterClass.CreateUserResponse toProto() {
    return UserManagementServiceOuterClass.CreateUserResponse.newBuilder()
        .setUser(this.user.toProto())
        .build();
  }

  @Override
  public String toString() {
    return "CreateUserResponse{" + "user=" + user + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateUserResponse that = (CreateUserResponse) o;
    return user.equals(that.user);
  }

  @Override
  public int hashCode() {
    return Objects.hash(user);
  }
}
