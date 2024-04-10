// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import java.util.List;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class GetTransactionByIdRequest {

  @NonNull private final String updateId;

  @NonNull private final List<@NonNull String> requestingParties;

  public GetTransactionByIdRequest(
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

  public static GetTransactionByIdRequest fromProto(
      UpdateServiceOuterClass.GetTransactionByIdRequest request) {
    return new GetTransactionByIdRequest(request.getUpdateId(), request.getRequestingPartiesList());
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
    GetTransactionByIdRequest that = (GetTransactionByIdRequest) o;
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
