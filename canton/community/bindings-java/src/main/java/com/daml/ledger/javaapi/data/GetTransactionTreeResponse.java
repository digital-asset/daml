// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class GetTransactionTreeResponse {

  @NonNull private final TransactionTree transactionTree;

  private GetTransactionTreeResponse(@NonNull TransactionTree transactionTree) {
    this.transactionTree = transactionTree;
  }

  public TransactionTree getTransactionTree() {
    return transactionTree;
  }

  public static GetTransactionTreeResponse fromProto(
      UpdateServiceOuterClass.GetTransactionTreeResponse response) {
    return new GetTransactionTreeResponse(TransactionTree.fromProto(response.getTransaction()));
  }

  public UpdateServiceOuterClass.GetTransactionTreeResponse toProto() {
    return UpdateServiceOuterClass.GetTransactionTreeResponse.newBuilder()
        .setTransaction(transactionTree.toProto())
        .build();
  }

  @Override
  public String toString() {
    return "GetTransactionTreeResponse{" + "transactionTree=" + transactionTree + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetTransactionTreeResponse that = (GetTransactionTreeResponse) o;
    return Objects.equals(transactionTree, that.transactionTree);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transactionTree);
  }
}
