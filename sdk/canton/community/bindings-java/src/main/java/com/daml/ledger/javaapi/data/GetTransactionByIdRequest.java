// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public final class GetTransactionByIdRequest {

  @NonNull private final String updateId;

  @NonNull private final TransactionFormat transactionFormat;

  public GetTransactionByIdRequest(
      @NonNull String updateId, @NonNull List<@NonNull String> requestingParties) {
    this.updateId = updateId;
    Map<String, Filter> partyFilters =
        requestingParties.stream()
            .collect(
                Collectors.toMap(
                    party -> party,
                    party ->
                        new CumulativeFilter(
                            Map.of(),
                            Map.of(),
                            Optional.of(Filter.Wildcard.HIDE_CREATED_EVENT_BLOB))));
    EventFormat eventFormat = new EventFormat(partyFilters, Optional.empty(), true);
    this.transactionFormat = new TransactionFormat(eventFormat, TransactionShape.ACS_DELTA);
  }

  public GetTransactionByIdRequest(
      @NonNull String updateId, @NonNull TransactionFormat transactionFormat) {
    this.updateId = updateId;
    this.transactionFormat = transactionFormat;
  }

  @NonNull
  public String getUpdateId() {
    return updateId;
  }

  @NonNull
  public List<@NonNull String> getRequestingParties() {
    return transactionFormat.getEventFormat().getParties().stream().toList();
  }

  @NonNull
  public TransactionFormat getTransactionFormat() {
    return transactionFormat;
  }

  public static GetTransactionByIdRequest fromProto(
      UpdateServiceOuterClass.GetTransactionByIdRequest request) {
    if (request.hasTransactionFormat()) {
      if (!request.getRequestingPartiesList().isEmpty())
        throw new IllegalArgumentException(
            "Request has both transactionFormat and requestingParties defined.");
      return new GetTransactionByIdRequest(
          request.getUpdateId(), TransactionFormat.fromProto(request.getTransactionFormat()));
    } else {
      if (request.getRequestingPartiesList().isEmpty())
        throw new IllegalArgumentException(
            "Request has neither transactionFormat nor requestingParties defined.");
      return new GetTransactionByIdRequest(
          request.getUpdateId(), request.getRequestingPartiesList());
    }
  }

  public UpdateServiceOuterClass.GetTransactionByIdRequest toProto() {
    return UpdateServiceOuterClass.GetTransactionByIdRequest.newBuilder()
        .setUpdateId(updateId)
        .setTransactionFormat(transactionFormat.toProto())
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetTransactionByIdRequest that = (GetTransactionByIdRequest) o;
    return Objects.equals(updateId, that.updateId)
        && Objects.equals(transactionFormat, that.transactionFormat);
  }

  @Override
  public int hashCode() {
    return Objects.hash(updateId, transactionFormat);
  }

  @Override
  public String toString() {
    return "GetTransactionByIdRequest{"
        + "updateId="
        + updateId
        + ", transactionFormat="
        + transactionFormat
        + '}';
  }
}
