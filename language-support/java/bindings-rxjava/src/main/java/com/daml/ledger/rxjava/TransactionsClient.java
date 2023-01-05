// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.*;
import io.reactivex.Flowable;
import io.reactivex.Single;
import java.util.Set;

/** An RxJava version of {@link com.daml.ledger.api.v1.TransactionServiceGrpc} */
public interface TransactionsClient {

  Flowable<Transaction> getTransactions(
      LedgerOffset begin, LedgerOffset end, TransactionFilter filter, boolean verbose);

  Flowable<Transaction> getTransactions(
      LedgerOffset begin,
      LedgerOffset end,
      TransactionFilter filter,
      boolean verbose,
      String accessToken);

  Flowable<Transaction> getTransactions(
      LedgerOffset begin, TransactionFilter filter, boolean verbose);

  Flowable<Transaction> getTransactions(
      LedgerOffset begin, TransactionFilter filter, boolean verbose, String accessToken);

  Flowable<Transaction> getTransactions(
      ContractFilter<?> contractFilter, LedgerOffset begin, Set<String> parties, boolean verbose);

  Flowable<Transaction> getTransactions(
      ContractFilter<?> contractFilter,
      LedgerOffset begin,
      Set<String> parties,
      boolean verbose,
      String accessToken);

  Flowable<TransactionTree> getTransactionsTrees(
      LedgerOffset begin, LedgerOffset end, TransactionFilter filter, boolean verbose);

  Flowable<TransactionTree> getTransactionsTrees(
      LedgerOffset begin,
      LedgerOffset end,
      TransactionFilter filter,
      boolean verbose,
      String accessToken);

  Flowable<TransactionTree> getTransactionsTrees(
      LedgerOffset begin, TransactionFilter filter, boolean verbose);

  Flowable<TransactionTree> getTransactionsTrees(
      LedgerOffset begin, TransactionFilter filter, boolean verbose, String accessToken);

  Single<TransactionTree> getTransactionByEventId(String eventId, Set<String> requestingParties);

  Single<TransactionTree> getTransactionByEventId(
      String eventId, Set<String> requestingParties, String accessToken);

  Single<TransactionTree> getTransactionById(String transactionId, Set<String> requestingParties);

  Single<TransactionTree> getTransactionById(
      String transactionId, Set<String> requestingParties, String accessToken);

  Single<Transaction> getFlatTransactionByEventId(String eventId, Set<String> requestingParties);

  Single<Transaction> getFlatTransactionByEventId(
      String eventId, Set<String> requestingParties, String accessToken);

  Single<Transaction> getFlatTransactionById(String transactionId, Set<String> requestingParties);

  Single<Transaction> getFlatTransactionById(
      String transactionId, Set<String> requestingParties, String accessToken);

  Single<LedgerOffset> getLedgerEnd();

  Single<LedgerOffset> getLedgerEnd(String accessToken);
}
