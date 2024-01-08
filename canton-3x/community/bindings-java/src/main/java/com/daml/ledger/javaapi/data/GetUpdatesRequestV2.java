// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class GetUpdatesRequestV2 {

  @NonNull private final ParticipantOffsetV2 beginExclusive;

  @NonNull private final ParticipantOffsetV2 endInclusive;

  @NonNull private final TransactionFilterV2 transactionFilter;

  private final boolean verbose;

  public GetUpdatesRequestV2(
      @NonNull ParticipantOffsetV2 beginExclusive,
      @NonNull ParticipantOffsetV2 endInclusive,
      @NonNull TransactionFilterV2 transactionFilter,
      boolean verbose) {
    this.beginExclusive = beginExclusive;
    this.endInclusive = endInclusive;
    this.transactionFilter = transactionFilter;
    this.verbose = verbose;
  }

  public static GetUpdatesRequestV2 fromProto(UpdateServiceOuterClass.GetUpdatesRequest request) {
    TransactionFilterV2 filters = TransactionFilterV2.fromProto(request.getFilter());
    boolean verbose = request.getVerbose();
    return new GetUpdatesRequestV2(
        ParticipantOffsetV2.fromProto(request.getBeginExclusive()),
        ParticipantOffsetV2.fromProto(request.getEndInclusive()),
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
  public ParticipantOffsetV2 getBeginExclusive() {
    return beginExclusive;
  }

  @NonNull
  public ParticipantOffsetV2 getEndInclusive() {
    return endInclusive;
  }

  @NonNull
  public TransactionFilterV2 getTransactionFilter() {
    return transactionFilter;
  }

  public boolean isVerbose() {
    return verbose;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetUpdatesRequestV2 that = (GetUpdatesRequestV2) o;
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
