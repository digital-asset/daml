// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class GetTransactionTreeResponseV2 {

  @NonNull private final TransactionTreeV2 transactionTree;

  private GetTransactionTreeResponseV2(@NonNull TransactionTreeV2 transactionTree) {
    this.transactionTree = transactionTree;
  }

  public TransactionTreeV2 getTransactionTree() {
    return transactionTree;
  }

  public static GetTransactionTreeResponseV2 fromProto(
      UpdateServiceOuterClass.GetTransactionTreeResponse response) {
    return new GetTransactionTreeResponseV2(TransactionTreeV2.fromProto(response.getTransaction()));
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
    GetTransactionTreeResponseV2 that = (GetTransactionTreeResponseV2) o;
    return Objects.equals(transactionTree, that.transactionTree);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transactionTree);
  }
}
