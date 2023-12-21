// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class GetActiveContractsRequestV2 {

  @NonNull private final TransactionFilterV2 transactionFilter;

  private final boolean verbose;

  @NonNull private final String activeAtOffset;

  public GetActiveContractsRequestV2(
      @NonNull TransactionFilterV2 transactionFilter,
      boolean verbose,
      @NonNull String activeAtOffset) {
    this.transactionFilter = transactionFilter;
    this.verbose = verbose;
    this.activeAtOffset = activeAtOffset;
  }

  public static GetActiveContractsRequestV2 fromProto(
      StateServiceOuterClass.GetActiveContractsRequest request) {
    TransactionFilterV2 filters = TransactionFilterV2.fromProto(request.getFilter());
    boolean verbose = request.getVerbose();
    String activeAtOffset = request.getActiveAtOffset();
    return new GetActiveContractsRequestV2(filters, verbose, activeAtOffset);
  }

  public StateServiceOuterClass.GetActiveContractsRequest toProto() {
    return StateServiceOuterClass.GetActiveContractsRequest.newBuilder()
        .setFilter(this.transactionFilter.toProto())
        .setVerbose(this.verbose)
        .setActiveAtOffset(this.activeAtOffset)
        .build();
  }

  @NonNull
  public TransactionFilterV2 getTransactionFilter() {
    return transactionFilter;
  }

  public boolean isVerbose() {
    return verbose;
  }

  @NonNull
  public String getActiveAtOffset() {
    return activeAtOffset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetActiveContractsRequestV2 that = (GetActiveContractsRequestV2) o;
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
