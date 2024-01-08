// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.TransactionServiceOuterClass;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class GetTransactionResponse {

  private final TransactionTree transaction;

  public GetTransactionResponse(@NonNull TransactionTree transaction) {
    this.transaction = transaction;
  }

  public static GetTransactionResponse fromProto(
      TransactionServiceOuterClass.GetTransactionResponse response) {
    return new GetTransactionResponse(TransactionTree.fromProto(response.getTransaction()));
  }

  public TransactionServiceOuterClass.GetTransactionResponse toProto() {
    return TransactionServiceOuterClass.GetTransactionResponse.newBuilder()
        .setTransaction(this.transaction.toProto())
        .build();
  }

  public TransactionTree getTransaction() {
    return transaction;
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
