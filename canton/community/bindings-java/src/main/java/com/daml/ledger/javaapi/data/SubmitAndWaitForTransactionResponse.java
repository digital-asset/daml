// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class SubmitAndWaitForTransactionResponse {

  @NonNull private final Transaction transaction;

  @NonNull private final String completionOffset;

  private SubmitAndWaitForTransactionResponse(
      @NonNull Transaction transaction, @NonNull String completionOffset) {
    this.transaction = transaction;
    this.completionOffset = completionOffset;
  }

  @NonNull
  public Transaction getTransaction() {
    return transaction;
  }

  @NonNull
  public String getCompletionOffset() {
    return completionOffset;
  }

  public static SubmitAndWaitForTransactionResponse fromProto(
      CommandServiceOuterClass.SubmitAndWaitForTransactionResponse response) {
    return new SubmitAndWaitForTransactionResponse(
        Transaction.fromProto(response.getTransaction()), response.getCompletionOffset());
  }

  public CommandServiceOuterClass.SubmitAndWaitForTransactionResponse toProto() {
    return CommandServiceOuterClass.SubmitAndWaitForTransactionResponse.newBuilder()
        .setTransaction(transaction.toProto())
        .setCompletionOffset(completionOffset)
        .build();
  }

  @Override
  public String toString() {

    return "SubmitAndWaitForTransactionResponse{"
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
    SubmitAndWaitForTransactionResponse that = (SubmitAndWaitForTransactionResponse) o;
    return Objects.equals(transaction, that.transaction)
        && Objects.equals(completionOffset, that.completionOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transaction, completionOffset);
  }
}
