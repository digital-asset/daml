// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

/**
 * Helper wrapper of a grpc message used in SubmitAndWaitForTransactionTree call. Class will be
 * removed in 3.4.0.
 */
public final class SubmitAndWaitForTransactionTreeResponse {

  @NonNull private final TransactionTree transaction;

  private SubmitAndWaitForTransactionTreeResponse(@NonNull TransactionTree transaction) {
    this.transaction = transaction;
  }

  @NonNull
  public TransactionTree getTransaction() {
    return transaction;
  }

  public static SubmitAndWaitForTransactionTreeResponse fromProto(
      CommandServiceOuterClass.SubmitAndWaitForTransactionTreeResponse response) {
    return new SubmitAndWaitForTransactionTreeResponse(
        TransactionTree.fromProto(response.getTransaction()));
  }

  public CommandServiceOuterClass.SubmitAndWaitForTransactionTreeResponse toProto() {
    return CommandServiceOuterClass.SubmitAndWaitForTransactionTreeResponse.newBuilder()
        .setTransaction(transaction.toProto())
        .build();
  }

  @Override
  public String toString() {

    return "SubmitAndWaitForTransactionTreeResponse{" + "transaction=" + transaction + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SubmitAndWaitForTransactionTreeResponse that = (SubmitAndWaitForTransactionTreeResponse) o;
    return Objects.equals(transaction, that.transaction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transaction);
  }
}
