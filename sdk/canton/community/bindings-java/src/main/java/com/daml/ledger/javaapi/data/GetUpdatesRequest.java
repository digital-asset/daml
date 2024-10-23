// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

public final class GetUpdatesRequest {

  @NonNull private final Long beginExclusive;

  @NonNull private final Optional<Long> endInclusive;

  @NonNull private final TransactionFilter transactionFilter;

  private final boolean verbose;

  public GetUpdatesRequest(
      @NonNull Long beginExclusive,
      @NonNull Optional<Long> endInclusive,
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
        request.getBeginExclusive(),
        request.hasEndInclusive() ? Optional.of(request.getEndInclusive()) : Optional.empty(),
        filters,
        verbose);
  }

  public UpdateServiceOuterClass.GetUpdatesRequest toProto() {
    UpdateServiceOuterClass.GetUpdatesRequest.Builder builder =
        UpdateServiceOuterClass.GetUpdatesRequest.newBuilder()
            .setBeginExclusive(beginExclusive)
            .setFilter(this.transactionFilter.toProto())
            .setVerbose(this.verbose);

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
