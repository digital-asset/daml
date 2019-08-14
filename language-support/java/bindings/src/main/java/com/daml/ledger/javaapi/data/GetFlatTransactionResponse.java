// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.TransactionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public class GetFlatTransactionResponse {

    private final Transaction transaction;

    public GetFlatTransactionResponse(@NonNull Transaction transaction) {
        this.transaction = transaction;
    }

    public static GetFlatTransactionResponse fromProto(TransactionServiceOuterClass.GetFlatTransactionResponse response) {
        return new GetFlatTransactionResponse(Transaction.fromProto(response.getTransaction()));
    }

    public TransactionServiceOuterClass.GetFlatTransactionResponse toProto() {
        return TransactionServiceOuterClass.GetFlatTransactionResponse.newBuilder()
                .setTransaction(this.transaction.toProto())
                .build();
    }

    public Transaction getTransaction() {
        return transaction;
    }

    @Override
    public String toString() {
        return "GetFlatTransactionResponse{" +
                "transaction=" + transaction +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetFlatTransactionResponse that = (GetFlatTransactionResponse) o;
        return Objects.equals(transaction, that.transaction);
    }

    @Override
    public int hashCode() {

        return Objects.hash(transaction);
    }
}
