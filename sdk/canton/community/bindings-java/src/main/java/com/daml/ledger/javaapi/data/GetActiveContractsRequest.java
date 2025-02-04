// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class GetActiveContractsRequest {

  @NonNull private final TransactionFilter transactionFilter;

  private final boolean verbose;

  @NonNull private final Long activeAtOffset;

  public GetActiveContractsRequest(
      @NonNull TransactionFilter transactionFilter, boolean verbose, @NonNull Long activeAtOffset) {
    this.transactionFilter = transactionFilter;
    this.verbose = verbose;
    this.activeAtOffset = activeAtOffset;
  }

  public static GetActiveContractsRequest fromProto(
      StateServiceOuterClass.GetActiveContractsRequest request) {
    TransactionFilter filters = TransactionFilter.fromProto(request.getFilter());
    boolean verbose = request.getVerbose();
    Long activeAtOffset = request.getActiveAtOffset();
    return new GetActiveContractsRequest(filters, verbose, activeAtOffset);
  }

  public StateServiceOuterClass.GetActiveContractsRequest toProto() {
    return StateServiceOuterClass.GetActiveContractsRequest.newBuilder()
        .setFilter(this.transactionFilter.toProto())
        .setVerbose(this.verbose)
        .setActiveAtOffset(this.activeAtOffset)
        .build();
  }

  @NonNull
  public TransactionFilter getTransactionFilter() {
    return transactionFilter;
  }

  public boolean isVerbose() {
    return verbose;
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
    return verbose == that.verbose
        && Objects.equals(transactionFilter, that.transactionFilter)
        && Objects.equals(activeAtOffset, that.activeAtOffset);
  }

  @Override
  public int hashCode() {

    return Objects.hash(transactionFilter, verbose, activeAtOffset);
  }

  @Override
  public String toString() {
    return "GetActiveContractsRequest{"
        + "transactionFilter="
        + transactionFilter
        + ", verbose="
        + verbose
        + ", activeAtOffset='"
        + activeAtOffset
        + '\''
        + '}';
  }
}
