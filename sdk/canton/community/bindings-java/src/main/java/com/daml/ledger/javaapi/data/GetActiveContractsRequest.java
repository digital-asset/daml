// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class GetActiveContractsRequest {

  @NonNull private final EventFormat eventFormat;

  @NonNull private final Long activeAtOffset;

  // TODO(i23504) remove
  public GetActiveContractsRequest(
      @NonNull TransactionFilter transactionFilter, boolean verbose, @NonNull Long activeAtOffset) {
    this.eventFormat =
        new EventFormat(
            transactionFilter.getPartyToFilters(), transactionFilter.getAnyPartyFilter(), verbose);
    this.activeAtOffset = activeAtOffset;
  }

  public GetActiveContractsRequest(@NonNull EventFormat eventFormat, @NonNull Long activeAtOffset) {
    this.eventFormat = eventFormat;
    this.activeAtOffset = activeAtOffset;
  }

  public static GetActiveContractsRequest fromProto(
      StateServiceOuterClass.GetActiveContractsRequest request) {
    if (request.hasEventFormat()) {
      if (request.hasFilter() || request.getVerbose())
        throw new IllegalArgumentException(
            "Request has both eventFormat and filter/verbose defined.");
      return new GetActiveContractsRequest(
          EventFormat.fromProto(request.getEventFormat()), request.getActiveAtOffset());
    } else {
      if (!request.hasFilter())
        throw new IllegalArgumentException("Request has neither eventFormat nor filter defined.");
      return new GetActiveContractsRequest(
          TransactionFilter.fromProto(request.getFilter()),
          request.getVerbose(),
          request.getActiveAtOffset());
    }
  }

  public StateServiceOuterClass.GetActiveContractsRequest toProto() {
    return StateServiceOuterClass.GetActiveContractsRequest.newBuilder()
        .setEventFormat(this.eventFormat.toProto())
        .setActiveAtOffset(this.activeAtOffset)
        .build();
  }

  // TODO(i23504) remove
  @NonNull
  public TransactionFilter getTransactionFilter() {
    return new TransactionFilter(eventFormat.getPartyToFilters(), eventFormat.getAnyPartyFilter());
  }

  // TODO(i23504) remove
  public boolean isVerbose() {
    return eventFormat.getVerbose();
  }

  @NonNull
  public Long getActiveAtOffset() {
    return activeAtOffset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetActiveContractsRequest that = (GetActiveContractsRequest) o;
    return Objects.equals(eventFormat, that.eventFormat)
        && Objects.equals(activeAtOffset, that.activeAtOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(eventFormat, activeAtOffset);
  }

  @Override
  public String toString() {
    return "GetActiveContractsRequest{"
        + "eventFormat="
        + eventFormat
        + ", activeAtOffset='"
        + activeAtOffset
        + '\''
        + '}';
  }
}
