// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class SubmitAndWaitForTransactionResponse {

  @NonNull private final Transaction transaction;

  private SubmitAndWaitForTransactionResponse(@NonNull Transaction transaction) {
    this.transaction = transaction;
  }

  @NonNull
  public Transaction getTransaction() {
    return transaction;
  }

  @NonNull
  public Long getCompletionOffset() {
    return transaction.getOffset();
  }

  public static SubmitAndWaitForTransactionResponse fromProto(
      CommandServiceOuterClass.SubmitAndWaitForTransactionResponse response) {
    return new SubmitAndWaitForTransactionResponse(
        Transaction.fromProto(response.getTransaction()));
  }

  public CommandServiceOuterClass.SubmitAndWaitForTransactionResponse toProto() {
    return CommandServiceOuterClass.SubmitAndWaitForTransactionResponse.newBuilder()
        .setTransaction(transaction.toProto())
        .build();
  }

  @Override
  public String toString() {

    return "SubmitAndWaitForTransactionResponse{" + "transaction=" + transaction + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SubmitAndWaitForTransactionResponse that = (SubmitAndWaitForTransactionResponse) o;
    return Objects.equals(transaction, that.transaction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transaction);
  }
}
