// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class SubmitAndWaitForTransactionTreeResponse {

  @NonNull private final TransactionTree transaction;

  @NonNull private final String completionOffset;

  private SubmitAndWaitForTransactionTreeResponse(
      @NonNull TransactionTree transaction, @NonNull String completionOffset) {
    this.transaction = transaction;
    this.completionOffset = completionOffset;
  }

  @NonNull
  public TransactionTree getTransaction() {
    return transaction;
  }

  @NonNull
  public String getCompletionOffset() {
    return completionOffset;
  }

  public static SubmitAndWaitForTransactionTreeResponse fromProto(
      CommandServiceOuterClass.SubmitAndWaitForTransactionTreeResponse response) {
    return new SubmitAndWaitForTransactionTreeResponse(
        TransactionTree.fromProto(response.getTransaction()), response.getCompletionOffset());
  }

  public CommandServiceOuterClass.SubmitAndWaitForTransactionTreeResponse toProto() {
    return CommandServiceOuterClass.SubmitAndWaitForTransactionTreeResponse.newBuilder()
        .setTransaction(transaction.toProto())
        .setCompletionOffset(completionOffset)
        .build();
  }

  @Override
  public String toString() {

    return "SubmitAndWaitForTransactionTreeResponse{"
        + "transaction="
        + transaction
        + ", completionOffset='"
        + completionOffset
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SubmitAndWaitForTransactionTreeResponse that = (SubmitAndWaitForTransactionTreeResponse) o;
    return Objects.equals(transaction, that.transaction)
        && Objects.equals(completionOffset, that.completionOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transaction, completionOffset);
  }
}
