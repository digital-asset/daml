// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

public final class GetUpdatesRequest {

  @NonNull private final String beginExclusive;

  @NonNull private final String endInclusive;

  @NonNull private final TransactionFilter transactionFilter;

  private final boolean verbose;

  public GetUpdatesRequest(
      @NonNull String beginExclusive,
      @NonNull String endInclusive,
      @NonNull TransactionFilter transactionFilter,
      boolean verbose) {
    this.beginExclusive = beginExclusive;
    this.endInclusive = endInclusive;
    this.transactionFilter = transactionFilter;
    this.verbose = verbose;
  }

  public static GetUpdatesRequest fromProto(UpdateServiceOuterClass.GetUpdatesRequest request) {
    TransactionFilter filters = TransactionFilter.fromProto(request.getFilter());
    boolean verbose = request.getVerbose();
    return new GetUpdatesRequest(
        request.getBeginExclusive(), request.getEndInclusive(), filters, verbose);
  }

  public UpdateServiceOuterClass.GetUpdatesRequest toProto() {
    return UpdateServiceOuterClass.GetUpdatesRequest.newBuilder()
        .setBeginExclusive(beginExclusive)
        .setEndInclusive(endInclusive)
        .setFilter(this.transactionFilter.toProto())
        .setVerbose(this.verbose)
        .build();
  }

  @NonNull
  public String getBeginExclusive() {
    return beginExclusive;
  }

  @NonNull
  public String getEndInclusive() {
    return endInclusive;
  }

  @NonNull
  public TransactionFilter getTransactionFilter() {
    return transactionFilter;
  }

  public boolean isVerbose() {
    return verbose;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetUpdatesRequest that = (GetUpdatesRequest) o;
    return Objects.equals(beginExclusive, that.beginExclusive)
        && Objects.equals(endInclusive, that.endInclusive)
        && Objects.equals(transactionFilter, that.transactionFilter)
        && verbose == that.verbose;
  }

  @Override
  public int hashCode() {

    return Objects.hash(beginExclusive, endInclusive, transactionFilter, verbose);
  }

  @Override
  public String toString() {
    return "GetUpdatesRequest{"
        + "beginExclusive="
        + beginExclusive
        + ", endInclusive="
        + endInclusive
        + ", transactionFilter="
        + transactionFilter
        + ", verbose="
        + verbose
        + '}';
  }
}
