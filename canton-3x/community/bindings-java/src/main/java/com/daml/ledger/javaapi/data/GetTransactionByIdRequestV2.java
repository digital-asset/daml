// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class GetTransactionByIdRequestV2 {

  @NonNull private final String updateId;

  @NonNull private final List<@NonNull String> requestingParties;

  public GetTransactionByIdRequestV2(
      @NonNull String updateId, @NonNull List<@NonNull String> requestingParties) {
    this.updateId = updateId;
    this.requestingParties = List.copyOf(requestingParties);
  }

  @NonNull
  public String getUpdateId() {
    return updateId;
  }

  @NonNull
  public List<@NonNull String> getRequestingParties() {
    return requestingParties;
  }

  public static GetTransactionByIdRequestV2 fromProto(
      UpdateServiceOuterClass.GetTransactionByIdRequest request) {
    return new GetTransactionByIdRequestV2(
        request.getUpdateId(), request.getRequestingPartiesList());
  }

  public UpdateServiceOuterClass.GetTransactionByIdRequest toProto() {
    return UpdateServiceOuterClass.GetTransactionByIdRequest.newBuilder()
        .setUpdateId(updateId)
        .addAllRequestingParties(requestingParties)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetTransactionByIdRequestV2 that = (GetTransactionByIdRequestV2) o;
    return Objects.equals(updateId, that.updateId)
        && Objects.equals(requestingParties, that.requestingParties);
  }

  @Override
  public int hashCode() {

    return Objects.hash(updateId, requestingParties);
  }

  @Override
  public String toString() {
    return "GetTransactionByEventIdRequest{"
        + "updateId="
        + updateId
        + ", requestingParties="
        + requestingParties
        + '}';
  }
}
