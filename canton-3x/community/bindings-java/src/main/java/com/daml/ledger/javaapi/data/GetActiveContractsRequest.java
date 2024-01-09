// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.ActiveContractsServiceOuterClass;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class GetActiveContractsRequest {

  private final String ledgerId;

  private final TransactionFilter transactionFilter;

  private final boolean verbose;

  public GetActiveContractsRequest(
      @NonNull String ledgerId,
      @NonNull TransactionFilter transactionFilter,
      @NonNull boolean verbose) {
    this.ledgerId = ledgerId;
    this.transactionFilter = transactionFilter;
    this.verbose = verbose;
  }

  public static GetActiveContractsRequest fromProto(
      ActiveContractsServiceOuterClass.GetActiveContractsRequest request) {
    String ledgerId = request.getLedgerId();
    TransactionFilter filters = TransactionFilter.fromProto(request.getFilter());
    boolean verbose = request.getVerbose();
    return new GetActiveContractsRequest(ledgerId, filters, verbose);
  }

  public ActiveContractsServiceOuterClass.GetActiveContractsRequest toProto() {
    return ActiveContractsServiceOuterClass.GetActiveContractsRequest.newBuilder()
        .setLedgerId(this.ledgerId)
        .setFilter(this.transactionFilter.toProto())
        .setVerbose(this.verbose)
        .build();
  }

  @NonNull
  public String getLedgerId() {
    return ledgerId;
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
    GetActiveContractsRequest that = (GetActiveContractsRequest) o;
    return verbose == that.verbose
        && Objects.equals(ledgerId, that.ledgerId)
        && Objects.equals(transactionFilter, that.transactionFilter);
  }

  @Override
  public int hashCode() {

    return Objects.hash(ledgerId, transactionFilter, verbose);
  }

  @Override
  public String toString() {
    return "GetActiveContractsRequest{"
        + "ledgerId='"
        + ledgerId
        + '\''
        + ", transactionFilter="
        + transactionFilter
        + ", verbose="
        + verbose
        + '}';
  }
}
