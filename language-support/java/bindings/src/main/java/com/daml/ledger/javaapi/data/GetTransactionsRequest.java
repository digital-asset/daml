// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.TransactionServiceOuterClass;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class GetTransactionsRequest {

  private final String ledgerId;

  private final LedgerOffset begin;

  private final Optional<LedgerOffset> end;

  private final TransactionFilter filter;

  private final boolean verbose;

  public GetTransactionsRequest(
      @NonNull String ledgerId,
      @NonNull LedgerOffset begin,
      @NonNull LedgerOffset end,
      @NonNull TransactionFilter filter,
      boolean verbose) {
    this.ledgerId = ledgerId;
    this.begin = begin;
    this.end = Optional.of(end);
    this.filter = filter;
    this.verbose = verbose;
  }

  public GetTransactionsRequest(
      @NonNull String ledgerId,
      @NonNull LedgerOffset begin,
      @NonNull TransactionFilter filter,
      boolean verbose) {
    this.ledgerId = ledgerId;
    this.begin = begin;
    this.end = Optional.empty();
    this.filter = filter;
    this.verbose = verbose;
  }

  public static GetTransactionsRequest fromProto(
      TransactionServiceOuterClass.GetTransactionsRequest request) {
    String ledgerId = request.getLedgerId();
    LedgerOffset begin = LedgerOffset.fromProto(request.getBegin());
    TransactionFilter filter = TransactionFilter.fromProto(request.getFilter());
    boolean verbose = request.getVerbose();
    if (request.hasEnd()) {
      LedgerOffset end = LedgerOffset.fromProto(request.getEnd());
      return new GetTransactionsRequest(ledgerId, begin, end, filter, verbose);
    } else {
      return new GetTransactionsRequest(ledgerId, begin, filter, verbose);
    }
  }

  public TransactionServiceOuterClass.GetTransactionsRequest toProto() {
    TransactionServiceOuterClass.GetTransactionsRequest.Builder builder =
        TransactionServiceOuterClass.GetTransactionsRequest.newBuilder();
    builder.setLedgerId(this.ledgerId);
    builder.setBegin(this.begin.toProto());
    this.end.ifPresent(end -> builder.setEnd(end.toProto()));
    builder.setFilter(this.filter.toProto());
    builder.setVerbose(this.verbose);
    return builder.build();
  }
}
