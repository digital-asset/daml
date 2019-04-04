// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.LedgerOffset;
import com.daml.ledger.javaapi.data.Transaction;
import com.daml.ledger.javaapi.data.TransactionFilter;
import com.daml.ledger.javaapi.data.TransactionTree;
import io.reactivex.Flowable;
import io.reactivex.Single;

import java.util.Set;

/**
 * An RxJava version of {@link com.digitalasset.ledger.api.v1.TransactionServiceGrpc}
 */
public interface TransactionsClient {

    Flowable<Transaction> getTransactions(LedgerOffset begin, LedgerOffset end, TransactionFilter filter, boolean verbose);

    Flowable<Transaction> getTransactions(LedgerOffset begin, TransactionFilter filter, boolean verbose);

    Flowable<TransactionTree> getTransactionsTrees(LedgerOffset begin, LedgerOffset end, TransactionFilter filter, boolean verbose);

    Flowable<TransactionTree> getTransactionsTrees(LedgerOffset begin, TransactionFilter filter, boolean verbose);

    Single<TransactionTree> getTransactionByEventId(String eventId, Set<String> requestingParties);

    Single<TransactionTree> getTransactionById(String transactionId, Set<String> requestingParties);

    Single<LedgerOffset> getLedgerEnd();

}
