// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class GetUpdateByIdRequest {

  @NonNull private final String updateId;

  @NonNull private final UpdateFormat updateFormat;

  public GetUpdateByIdRequest(@NonNull String updateId, @NonNull UpdateFormat updateFormat) {
    this.updateId = updateId;
    this.updateFormat = updateFormat;
  }

  @NonNull
  public String getUpdateId() {
    return updateId;
  }

  @NonNull
  public UpdateFormat getUpdateFormat() {
    return updateFormat;
  }

  public static GetUpdateByIdRequest fromProto(
      UpdateServiceOuterClass.GetUpdateByIdRequest request) {
    return new GetUpdateByIdRequest(
        request.getUpdateId(), UpdateFormat.fromProto(request.getUpdateFormat()));
  }

  public UpdateServiceOuterClass.GetUpdateByIdRequest toProto() {
    return UpdateServiceOuterClass.GetUpdateByIdRequest.newBuilder()
        .setUpdateId(updateId)
        .setUpdateFormat(updateFormat.toProto())
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetUpdateByIdRequest that = (GetUpdateByIdRequest) o;
    return Objects.equals(updateId, that.updateId)
        && Objects.equals(updateFormat, that.updateFormat);
  }

  @Override
  public int hashCode() {
    return Objects.hash(updateId, updateFormat);
  }

  @Override
  public String toString() {
    return "GetUpdateByIdRequest{"
        + "updateId="
        + updateId
        + ", updateFormat="
        + updateFormat
        + '}';
  }
}
