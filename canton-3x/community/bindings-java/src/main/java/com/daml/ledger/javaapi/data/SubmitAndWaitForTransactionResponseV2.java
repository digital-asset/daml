// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class SubmitAndWaitForTransactionResponseV2 {

  @NonNull private final TransactionV2 transaction;

  @NonNull private final String completionOffset;

  private SubmitAndWaitForTransactionResponseV2(
      @NonNull TransactionV2 transaction, @NonNull String completionOffset) {
    this.transaction = transaction;
    this.completionOffset = completionOffset;
  }

  @NonNull
  public TransactionV2 getTransaction() {
    return transaction;
  }

  @NonNull
  public String getCompletionOffset() {
    return completionOffset;
  }

  public static SubmitAndWaitForTransactionResponseV2 fromProto(
      CommandServiceOuterClass.SubmitAndWaitForTransactionResponse response) {
    return new SubmitAndWaitForTransactionResponseV2(
        TransactionV2.fromProto(response.getTransaction()), response.getCompletionOffset());
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
    SubmitAndWaitForTransactionResponseV2 that = (SubmitAndWaitForTransactionResponseV2) o;
    return Objects.equals(transaction, that.transaction)
        && Objects.equals(completionOffset, that.completionOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transaction, completionOffset);
  }
}
