// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.admin.UserManagementServiceOuterClass;
import java.util.Objects;

public final class GetUserResponse {

  private final User user;

  public GetUserResponse(User user) {
    this.user = user;
  }

  public User getUser() {
    return user;
  }

  public static GetUserResponse fromProto(UserManagementServiceOuterClass.GetUserResponse proto) {
    return new GetUserResponse(User.fromProto(proto.getUser()));
  }

  public UserManagementServiceOuterClass.GetUserResponse toProto() {
    return UserManagementServiceOuterClass.GetUserResponse.newBuilder()
        .setUser(this.user.toProto())
        .build();
  }

  @Override
  public String toString() {
    return "GetUserResponse{" + "user=" + user + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetUserResponse that = (GetUserResponse) o;
    return user.equals(that.user);
  }

  @Override
  public int hashCode() {
    return Objects.hash(user);
  }
}
