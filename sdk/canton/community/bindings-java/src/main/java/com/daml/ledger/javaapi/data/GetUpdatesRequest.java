// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class GetUpdatesRequest {

  @NonNull private final Long beginExclusive;

  @NonNull private final Optional<Long> endInclusive;

  @NonNull private final UpdateFormat updateFormat;

  public GetUpdatesRequest(
      @NonNull Long beginExclusive,
      @NonNull Optional<Long> endInclusive,
      @NonNull UpdateFormat updateFormat) {
    this.beginExclusive = beginExclusive;
    this.endInclusive = endInclusive;
    this.updateFormat = updateFormat;
  }

  public static GetUpdatesRequest fromProto(UpdateServiceOuterClass.GetUpdatesRequest request) {
    if (request.hasUpdateFormat()) {
      return new GetUpdatesRequest(
          request.getBeginExclusive(),
          request.hasEndInclusive() ? Optional.of(request.getEndInclusive()) : Optional.empty(),
          UpdateFormat.fromProto(request.getUpdateFormat()));
    } else {
      throw new IllegalArgumentException("Request has no updateFormat defined.");
    }
  }

  public UpdateServiceOuterClass.GetUpdatesRequest toProto() {
    UpdateServiceOuterClass.GetUpdatesRequest.Builder builder =
        UpdateServiceOuterClass.GetUpdatesRequest.newBuilder()
            .setBeginExclusive(beginExclusive)
            .setUpdateFormat(this.updateFormat.toProto());

    endInclusive.ifPresent(builder::setEndInclusive);
    return builder.build();
  }

  @NonNull
  public Long getBeginExclusive() {
    return beginExclusive;
  }

  @NonNull
  public Optional<Long> getEndInclusive() {
    return endInclusive;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetUpdatesRequest that = (GetUpdatesRequest) o;
    return Objects.equals(beginExclusive, that.beginExclusive)
        && Objects.equals(endInclusive, that.endInclusive)
        && Objects.equals(updateFormat, that.updateFormat);
  }

  @Override
  public int hashCode() {

    return Objects.hash(beginExclusive, endInclusive, updateFormat);
  }

  @Override
  public String toString() {
    return "GetUpdatesRequest{"
        + "beginExclusive="
        + beginExclusive
        + ", endInclusive="
        + endInclusive
        + ", updateFormat="
        + updateFormat
        + '}';
  }
}
