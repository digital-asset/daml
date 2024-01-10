// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.admin.UserManagementServiceOuterClass;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class GrantUserRightsRequest {

  private final String userId;
  private final List<User.Right> rights;

  public GrantUserRightsRequest(String userId, User.Right right, User.Right... rights) {
    this.userId = userId;
    this.rights = new ArrayList<>(rights.length + 1);
    this.rights.add(right);
    this.rights.addAll(Arrays.asList(rights));
  }

  public String getUserId() {
    return userId;
  }

  public List<User.Right> getRights() {
    return new ArrayList<>(rights);
  }

  public UserManagementServiceOuterClass.GrantUserRightsRequest toProto() {
    return UserManagementServiceOuterClass.GrantUserRightsRequest.newBuilder()
        .setUserId(this.userId)
        .addAllRights(this.rights.stream().map(User.Right::toProto).collect(Collectors.toList()))
        .build();
  }

  @Override
  public String toString() {
    return "GrantUserRightsRequest{" + "userId=" + userId + ", rights=" + rights + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GrantUserRightsRequest that = (GrantUserRightsRequest) o;
    return userId.equals(that.userId) && rights.equals(that.rights);
  }

  @Override
  public int hashCode() {
    return Objects.hash(userId, rights);
  }
}
