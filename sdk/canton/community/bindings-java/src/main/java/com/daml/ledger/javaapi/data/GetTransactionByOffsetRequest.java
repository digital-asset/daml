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

/**
 * Helper wrapper of a grpc message used in GetTransactionByOffset and GetTransactionTreeByOffset
 * calls. Class will be removed in 3.4.0.
 */
public final class GetTransactionByOffsetRequest {

  @NonNull private final Long offset;

  @NonNull private final TransactionFormat transactionFormat;

  public GetTransactionByOffsetRequest(
      @NonNull Long offset, @NonNull List<@NonNull String> requestingParties) {
    this.offset = offset;
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

  public GetTransactionByOffsetRequest(
      @NonNull Long offset, @NonNull TransactionFormat transactionFormat) {
    this.offset = offset;
    this.transactionFormat = transactionFormat;
  }

  @NonNull
  public Long getOffset() {
    return offset;
  }

  @NonNull
  public List<@NonNull String> getRequestingParties() {
    return transactionFormat.getEventFormat().getParties().stream().toList();
  }

  @NonNull
  public TransactionFormat getTransactionFormat() {
    return transactionFormat;
  }

  public static GetTransactionByOffsetRequest fromProto(
      UpdateServiceOuterClass.GetTransactionByOffsetRequest request) {
    if (request.hasTransactionFormat()) {
      if (!request.getRequestingPartiesList().isEmpty())
        throw new IllegalArgumentException(
            "Request has both transactionFormat and requestingParties defined.");
      return new GetTransactionByOffsetRequest(
          request.getOffset(), TransactionFormat.fromProto(request.getTransactionFormat()));
    } else {
      if (request.getRequestingPartiesList().isEmpty())
        throw new IllegalArgumentException(
            "Request has neither transactionFormat nor requestingParties defined.");
      return new GetTransactionByOffsetRequest(
          request.getOffset(), request.getRequestingPartiesList());
    }
  }

  public UpdateServiceOuterClass.GetTransactionByOffsetRequest toProto() {
    return UpdateServiceOuterClass.GetTransactionByOffsetRequest.newBuilder()
        .setOffset(offset)
        .setTransactionFormat(transactionFormat.toProto())
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetTransactionByOffsetRequest that = (GetTransactionByOffsetRequest) o;
    return Objects.equals(offset, that.offset)
        && Objects.equals(transactionFormat, that.transactionFormat);
  }

  @Override
  public int hashCode() {
    return Objects.hash(offset, transactionFormat);
  }

  @Override
  public String toString() {
    return "GetTransactionByOffsetRequest{"
        + "offset="
        + offset
        + ", transactionFormat="
        + transactionFormat
        + '}';
  }
}
