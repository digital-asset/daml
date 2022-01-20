// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.admin.UserManagementServiceOuterClass;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class ListUserRightsRequest {

  private final String userId;

  public ListUserRightsRequest(@NonNull String userId) {
    this.userId = userId;
  }

  public String getId() {
    return userId;
  }

  @Override
  public String toString() {
    return "ListUserRightsRequest{" + "userId='" + userId + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ListUserRightsRequest that = (ListUserRightsRequest) o;
    return Objects.equals(userId, that.userId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(userId);
  }

  public UserManagementServiceOuterClass.ListUserRightsRequest toProto() {
    return UserManagementServiceOuterClass.ListUserRightsRequest.newBuilder()
        .setUserId(this.userId)
        .build();
  }
}
