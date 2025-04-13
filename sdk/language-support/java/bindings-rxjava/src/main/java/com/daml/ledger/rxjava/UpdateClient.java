// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.*;
import io.reactivex.Flowable;
import io.reactivex.Single;
import java.util.Optional;
import java.util.Set;

/** An RxJava version of {@link com.daml.ledger.api.v2.UpdateServiceGrpc} */
public interface UpdateClient {

  Flowable<Transaction> getTransactions(
      ContractFilter<?> contractFilter,
      Long begin,
      Optional<Long> end,
      Set<String> parties,
      boolean verbose);

  Flowable<Transaction> getTransactions(
      Long begin, Optional<Long> end, TransactionFormat transactionFormat);

  Flowable<Transaction> getTransactions(
      Long begin, Optional<Long> end, TransactionFormat transactionFormat, String accessToken);

  // Method will be removed in 3.4
  Flowable<Transaction> getTransactions(
      Long begin, Optional<Long> end, TransactionFilter filter, boolean verbose);

  // Method will be removed in 3.4
  Flowable<Transaction> getTransactions(
      Long begin,
      Optional<Long> end,
      TransactionFilter filter,
      boolean verbose,
      String accessToken);

  // Method will be removed in 3.4
  Flowable<TransactionTree> getTransactionsTrees(
      Long begin, Optional<Long> end, TransactionFilter filter, boolean verbose);

  // Method will be removed in 3.4
  Flowable<TransactionTree> getTransactionsTrees(
      Long begin,
      Optional<Long> end,
      TransactionFilter filter,
      boolean verbose,
      String accessToken);

  // Method will be removed in 3.4
  Single<TransactionTree> getTransactionTreeByOffset(Long offset, Set<String> requestingParties);

  // Method will be removed in 3.4
  Single<TransactionTree> getTransactionTreeByOffset(
      Long offset, Set<String> requestingParties, String accessToken);

  // Method will be removed in 3.4
  Single<TransactionTree> getTransactionTreeById(
      String transactionId, Set<String> requestingParties);

  // Method will be removed in 3.4
  Single<TransactionTree> getTransactionTreeById(
      String transactionId, Set<String> requestingParties, String accessToken);

  Single<Transaction> getTransactionByOffset(Long offset, Set<String> requestingParties);

  Single<Transaction> getTransactionByOffset(
      Long offset, Set<String> requestingParties, String accessToken);

  Single<Transaction> getTransactionById(String transactionId, Set<String> requestingParties);

  Single<Transaction> getTransactionById(
      String transactionId, Set<String> requestingParties, String accessToken);
}
