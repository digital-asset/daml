// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.admin.UserManagementServiceOuterClass;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class DeleteUserRequest {

  private final String userId;

  public DeleteUserRequest(@NonNull String userId) {
    this.userId = userId;
  }

  public String getId() {
    return userId;
  }

  @Override
  public String toString() {
    return "DeleteUserRequest{" + "userId='" + userId + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DeleteUserRequest that = (DeleteUserRequest) o;
    return Objects.equals(userId, that.userId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(userId);
  }

  public UserManagementServiceOuterClass.DeleteUserRequest toProto() {
    return UserManagementServiceOuterClass.DeleteUserRequest.newBuilder()
        .setUserId(this.userId)
        .build();
  }
}
