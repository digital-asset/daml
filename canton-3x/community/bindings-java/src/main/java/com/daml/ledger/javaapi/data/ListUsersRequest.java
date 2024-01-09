// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.admin.UserManagementServiceOuterClass;
import java.util.Objects;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class ListUsersRequest {

  private final Optional<String> pageToken;

  private final Integer pageSize;

  public ListUsersRequest(@NonNull Optional<String> pageToken, @NonNull Integer pageSize) {
    this.pageToken = pageToken;
    this.pageSize = pageSize;
  }

  public Optional<String> getPageToken() {
    return pageToken;
  }

  public Integer getPageSize() {
    return pageSize;
  }

  @Override
  public String toString() {
    return "ListUsersRequest{" + "pageToken=" + pageToken + ", pageSize=" + pageSize + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ListUsersRequest that = (ListUsersRequest) o;
    return Objects.equals(pageToken, that.pageToken) && Objects.equals(pageSize, that.pageSize);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pageToken, pageSize);
  }

  public UserManagementServiceOuterClass.ListUsersRequest toProto() {
    UserManagementServiceOuterClass.ListUsersRequest.Builder builder =
        UserManagementServiceOuterClass.ListUsersRequest.newBuilder();
    pageToken.ifPresent(builder::setPageToken);
    builder.setPageSize(pageSize);
    return builder.build();
  }
}
