// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.TransactionOuterClass;
import com.digitalasset.ledger.api.v1.TransactionServiceOuterClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GetTransactionsResponse {

    private final List<Transaction> transactions;

    public GetTransactionsResponse(List<Transaction> transactions) {
        this.transactions = transactions;
    }

    public static GetTransactionsResponse fromProto(TransactionServiceOuterClass.GetTransactionsResponse response) {
        ArrayList<Transaction> transactions = new ArrayList<>(response.getTransactionsCount());
        for (TransactionOuterClass.Transaction transaction : response.getTransactionsList()) {
            transactions.add(Transaction.fromProto(transaction));
        }
        return new GetTransactionsResponse(transactions);
    }

    public TransactionServiceOuterClass.GetTransactionsResponse toProto() {
        ArrayList<TransactionOuterClass.Transaction> transactions = new ArrayList<>(this.transactions.size());
        for (Transaction transaction : this.transactions) {
            transactions.add(transaction.toProto());
        }
        return TransactionServiceOuterClass.GetTransactionsResponse.newBuilder()
                .addAllTransactions(transactions)
                .build();
    }

    public List<Transaction> getTransactions() {
        return transactions;
    }

    @Override
    public String toString() {
        return "GetTransactionsResponse{" +
                "transactions=" + transactions +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetTransactionsResponse that = (GetTransactionsResponse) o;
        return Objects.equals(transactions, that.transactions);
    }

    @Override
    public int hashCode() {

        return Objects.hash(transactions);
    }
}
