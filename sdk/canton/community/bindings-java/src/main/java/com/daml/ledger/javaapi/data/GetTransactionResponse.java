// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class GetTransactionResponse {

  @NonNull private final Transaction transaction;

  private GetTransactionResponse(@NonNull Transaction transaction) {
    this.transaction = transaction;
  }

  public Transaction getTransaction() {
    return transaction;
  }

  public static GetTransactionResponse fromProto(
      UpdateServiceOuterClass.GetTransactionResponse response) {
    return new GetTransactionResponse(Transaction.fromProto(response.getTransaction()));
  }

  public UpdateServiceOuterClass.GetTransactionResponse toProto() {
    return UpdateServiceOuterClass.GetTransactionResponse.newBuilder()
        .setTransaction(transaction.toProto())
        .build();
  }

  @Override
  public String toString() {
    return "GetTransactionResponse{" + "transaction=" + transaction + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetTransactionResponse that = (GetTransactionResponse) o;
    return Objects.equals(transaction, that.transaction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transaction);
  }
}
