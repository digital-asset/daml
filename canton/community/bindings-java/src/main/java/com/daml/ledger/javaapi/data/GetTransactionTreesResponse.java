// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.TransactionOuterClass;
import com.daml.ledger.api.v1.TransactionServiceOuterClass;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class GetTransactionTreesResponse {

  private final List<TransactionTree> transactions;

  public GetTransactionTreesResponse(@NonNull List<@NonNull TransactionTree> transactions) {
    this.transactions = transactions;
  }

  public static GetTransactionTreesResponse fromProto(
      TransactionServiceOuterClass.GetTransactionTreesResponse response) {
    ArrayList<TransactionTree> transactionTrees = new ArrayList<>(response.getTransactionsCount());
    for (TransactionOuterClass.TransactionTree transactionTree : response.getTransactionsList()) {
      transactionTrees.add(TransactionTree.fromProto(transactionTree));
    }
    return new GetTransactionTreesResponse(transactionTrees);
  }

  public TransactionServiceOuterClass.GetTransactionTreesResponse toProto() {
    ArrayList<TransactionOuterClass.TransactionTree> transactionTrees =
        new ArrayList<>(this.transactions.size());
    for (TransactionTree transactionTree : this.transactions) {
      transactionTrees.add(transactionTree.toProto());
    }
    return TransactionServiceOuterClass.GetTransactionTreesResponse.newBuilder()
        .addAllTransactions(transactionTrees)
        .build();
  }

  @NonNull
  public List<@NonNull TransactionTree> getTransactions() {
    return transactions;
  }

  @Override
  public String toString() {
    return "GetTransactionTreesResponse{" + "transactions=" + transactions + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetTransactionTreesResponse that = (GetTransactionTreesResponse) o;
    return Objects.equals(transactions, that.transactions);
  }

  @Override
  public int hashCode() {

    return Objects.hash(transactions);
  }
}
