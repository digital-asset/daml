// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class GetTransactionResponseV2 {

  @NonNull private final TransactionV2 transaction;

  private GetTransactionResponseV2(@NonNull TransactionV2 transaction) {
    this.transaction = transaction;
  }

  public TransactionV2 getTransaction() {
    return transaction;
  }

  public static GetTransactionResponseV2 fromProto(
      UpdateServiceOuterClass.GetTransactionResponse response) {
    return new GetTransactionResponseV2(TransactionV2.fromProto(response.getTransaction()));
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
    GetTransactionResponseV2 that = (GetTransactionResponseV2) o;
    return Objects.equals(transaction, that.transaction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transaction);
  }
}
