// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class SubmitAndWaitForTransactionTreeResponseV2 {

  @NonNull private final TransactionTreeV2 transaction;

  @NonNull private final String completionOffset;

  private SubmitAndWaitForTransactionTreeResponseV2(
      @NonNull TransactionTreeV2 transaction, @NonNull String completionOffset) {
    this.transaction = transaction;
    this.completionOffset = completionOffset;
  }

  @NonNull
  public TransactionTreeV2 getTransaction() {
    return transaction;
  }

  @NonNull
  public String getCompletionOffset() {
    return completionOffset;
  }

  public static SubmitAndWaitForTransactionTreeResponseV2 fromProto(
      CommandServiceOuterClass.SubmitAndWaitForTransactionTreeResponse response) {
    return new SubmitAndWaitForTransactionTreeResponseV2(
        TransactionTreeV2.fromProto(response.getTransaction()), response.getCompletionOffset());
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
    SubmitAndWaitForTransactionTreeResponseV2 that = (SubmitAndWaitForTransactionTreeResponseV2) o;
    return Objects.equals(transaction, that.transaction)
        && Objects.equals(completionOffset, that.completionOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transaction, completionOffset);
  }
}
