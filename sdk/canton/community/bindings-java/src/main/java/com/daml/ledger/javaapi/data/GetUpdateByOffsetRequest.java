// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class GetUpdateByOffsetRequest {

  @NonNull private final Long offset;

  @NonNull private final UpdateFormat updateFormat;

  public GetUpdateByOffsetRequest(@NonNull Long offset, @NonNull UpdateFormat updateFormat) {
    this.offset = offset;
    this.updateFormat = updateFormat;
  }

  @NonNull
  public Long getOffset() {
    return offset;
  }

  @NonNull
  public UpdateFormat getUpdateFormat() {
    return updateFormat;
  }

  public static GetUpdateByOffsetRequest fromProto(
      UpdateServiceOuterClass.GetUpdateByOffsetRequest request) {
    return new GetUpdateByOffsetRequest(
        request.getOffset(), UpdateFormat.fromProto(request.getUpdateFormat()));
  }

  public UpdateServiceOuterClass.GetUpdateByOffsetRequest toProto() {
    return UpdateServiceOuterClass.GetUpdateByOffsetRequest.newBuilder()
        .setOffset(offset)
        .setUpdateFormat(updateFormat.toProto())
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetUpdateByOffsetRequest that = (GetUpdateByOffsetRequest) o;
    return Objects.equals(offset, that.offset) && Objects.equals(updateFormat, that.updateFormat);
  }

  @Override
  public int hashCode() {
    return Objects.hash(offset, updateFormat);
  }

  @Override
  public String toString() {
    return "GetUpdateByOffsetRequest{"
        + "offset="
        + offset
        + ", updateFormat="
        + updateFormat
        + '}';
  }
}
