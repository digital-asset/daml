// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

  /**
   * Get contracts
   *
   * @param contractUtil Utilities for specified type of contract. It can be instantiated with
   *     <code>ContractTypeCompanion</code>
   * @param begin begin offset.
   * @param parties Set of parties to be included in the transaction filter.
   * @param verbose If enabled, values served over the API will contain more information than
   *     strictly necessary to interpret the data.
   * @return Flowable of contract type <code>Ct</code>
   */
  <Ct> Flowable<Ct> getContracts(
      ContractUtil<Ct> contractUtil, LedgerOffset begin, Set<String> parties, boolean verbose);

  /**
   * Get contracts
   *
   * @param contractUtil Utilities for specified type of contract. It can be instantiated with
   *     <code>ContractTypeCompanion</code>
   * @param begin begin offset.
   * @param parties Set of parties to be included in the transaction filter.
   * @param verbose If enabled, values served over the API will contain more information than
   *     strictly necessary to interpret the data.
   * @param accessToken Access token for authentication.
   * @return Flowable of contract type <code>Ct</code>
   */
  <Ct> Flowable<Ct> getContracts(
      ContractUtil<Ct> contractUtil,
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
