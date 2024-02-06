// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class GetUpdatesRequest {

  @NonNull private final ParticipantOffset beginExclusive;

  @NonNull private final ParticipantOffset endInclusive;

  @NonNull private final TransactionFilter transactionFilter;

  private final boolean verbose;

  public GetUpdatesRequest(
      @NonNull ParticipantOffset beginExclusive,
      @NonNull ParticipantOffset endInclusive,
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
        ParticipantOffset.fromProto(request.getBeginExclusive()),
        ParticipantOffset.fromProto(request.getEndInclusive()),
        filters,
        verbose);
  }

  public UpdateServiceOuterClass.GetUpdatesRequest toProto() {
    return UpdateServiceOuterClass.GetUpdatesRequest.newBuilder()
        .setBeginExclusive(beginExclusive.toProto())
        .setEndInclusive(endInclusive.toProto())
        .setFilter(this.transactionFilter.toProto())
        .setVerbose(this.verbose)
        .build();
  }

  @NonNull
  public ParticipantOffset getBeginExclusive() {
    return beginExclusive;
  }

  @NonNull
  public ParticipantOffset getEndInclusive() {
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
