// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandServiceOuterClass;
import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public final class SubmitAndWaitForTransactionRequest {

  @NonNull private final CommandsSubmission submission;

  @NonNull private final TransactionFormat transactionFormat;

  public SubmitAndWaitForTransactionRequest(
      @NonNull CommandsSubmission submission, @NonNull TransactionFormat transactionFormat) {
    this.submission = submission;
    this.transactionFormat = transactionFormat;
  }

  public SubmitAndWaitForTransactionRequest(@NonNull CommandsSubmission submission) {
    Map<String, Filter> partyFilters =
        submission.getActAs().stream()
            .collect(
                Collectors.toMap(
                    party -> party,
                    party ->
                        new CumulativeFilter(
                            Map.of(),
                            Map.of(),
                            Optional.of(Filter.Wildcard.HIDE_CREATED_EVENT_BLOB))));
    EventFormat eventFormat = new EventFormat(partyFilters, Optional.empty(), true);
    TransactionFormat transactionFormat =
        new TransactionFormat(eventFormat, TransactionShape.ACS_DELTA);
    this.submission = submission;
    this.transactionFormat = transactionFormat;
  }

  @NonNull
  public CommandsSubmission getCommandsSubmission() {
    return submission;
  }

  @NonNull
  public TransactionFormat getTransactionFormat() {
    return transactionFormat;
  }

  public CommandServiceOuterClass.SubmitAndWaitForTransactionRequest toProto() {
    return CommandServiceOuterClass.SubmitAndWaitForTransactionRequest.newBuilder()
        .setCommands(submission.toProto())
        .setTransactionFormat(transactionFormat.toProto())
        .build();
  }

  public static SubmitAndWaitForTransactionRequest fromProto(
      CommandServiceOuterClass.SubmitAndWaitForTransactionRequest request) {
    return new SubmitAndWaitForTransactionRequest(
        CommandsSubmission.fromProto(request.getCommands()),
        TransactionFormat.fromProto(request.getTransactionFormat()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SubmitAndWaitForTransactionRequest that = (SubmitAndWaitForTransactionRequest) o;
    return Objects.equals(submission, that.submission)
        && Objects.equals(transactionFormat, that.transactionFormat);
  }

  @Override
  public String toString() {
    return "SubmitAndWaitForTransactionRequest{"
        + "submission="
        + submission
        + ", transactionFormat="
        + transactionFormat
        + '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(submission, transactionFormat);
  }
}
