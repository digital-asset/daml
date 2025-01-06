// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;

public final class GetTransactionByOffsetRequest {

  @NonNull private final Long offset;

  @NonNull private final List<@NonNull String> requestingParties;

  public GetTransactionByOffsetRequest(
      @NonNull Long offset, @NonNull List<@NonNull String> requestingParties) {
    this.offset = offset;
    this.requestingParties = List.copyOf(requestingParties);
  }

  @NonNull
  public Long getOffset() {
    return offset;
  }

  @NonNull
  public List<@NonNull String> getRequestingParties() {
    return requestingParties;
  }

  public static GetTransactionByOffsetRequest fromProto(
      UpdateServiceOuterClass.GetTransactionByOffsetRequest request) {
    return new GetTransactionByOffsetRequest(
        request.getOffset(), request.getRequestingPartiesList());
  }

  public UpdateServiceOuterClass.GetTransactionByOffsetRequest toProto() {
    return UpdateServiceOuterClass.GetTransactionByOffsetRequest.newBuilder()
        .setOffset(offset)
        .addAllRequestingParties(requestingParties)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetTransactionByOffsetRequest that = (GetTransactionByOffsetRequest) o;
    return Objects.equals(offset, that.offset)
        && Objects.equals(requestingParties, that.requestingParties);
  }

  @Override
  public int hashCode() {

    return Objects.hash(offset, requestingParties);
  }

  @Override
  public String toString() {
    return "GetTransactionByOffsetRequest{"
        + "offset="
        + offset
        + ", requestingParties="
        + requestingParties
        + '}';
  }
}
