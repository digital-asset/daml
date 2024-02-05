// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.*;
import io.reactivex.Flowable;
import io.reactivex.Single;
import java.util.Set;

/** An RxJava version of {@link com.daml.ledger.api.v2.UpdateServiceGrpc} */
public interface UpdateClient {

  Flowable<TransactionV2> getTransactions(
      ParticipantOffsetV2 begin,
      ParticipantOffsetV2 end,
      TransactionFilterV2 filter,
      boolean verbose);

  Flowable<TransactionV2> getTransactions(
      ParticipantOffsetV2 begin,
      ParticipantOffsetV2 end,
      TransactionFilterV2 filter,
      boolean verbose,
      String accessToken);

  Flowable<TransactionV2> getTransactions(
      ParticipantOffsetV2 begin, TransactionFilterV2 filter, boolean verbose);

  Flowable<TransactionV2> getTransactions(
      ParticipantOffsetV2 begin, TransactionFilterV2 filter, boolean verbose, String accessToken);

  Flowable<TransactionV2> getTransactions(
      ContractFilter<?> contractFilter,
      ParticipantOffsetV2 begin,
      Set<String> parties,
      boolean verbose);

  Flowable<TransactionV2> getTransactions(
      ContractFilter<?> contractFilter,
      ParticipantOffsetV2 begin,
      Set<String> parties,
      boolean verbose,
      String accessToken);

  Flowable<TransactionTreeV2> getTransactionsTrees(
      ParticipantOffsetV2 begin,
      ParticipantOffsetV2 end,
      TransactionFilterV2 filter,
      boolean verbose);

  Flowable<TransactionTreeV2> getTransactionsTrees(
      ParticipantOffsetV2 begin,
      ParticipantOffsetV2 end,
      TransactionFilterV2 filter,
      boolean verbose,
      String accessToken);

  Flowable<TransactionTreeV2> getTransactionsTrees(
      ParticipantOffsetV2 begin, TransactionFilterV2 filter, boolean verbose);

  Flowable<TransactionTreeV2> getTransactionsTrees(
      ParticipantOffsetV2 begin, TransactionFilterV2 filter, boolean verbose, String accessToken);

  Single<TransactionTreeV2> getTransactionTreeByEventId(
      String eventId, Set<String> requestingParties);

  Single<TransactionTreeV2> getTransactionTreeByEventId(
      String eventId, Set<String> requestingParties, String accessToken);

  Single<TransactionTreeV2> getTransactionTreeById(
      String transactionId, Set<String> requestingParties);

  Single<TransactionTreeV2> getTransactionTreeById(
      String transactionId, Set<String> requestingParties, String accessToken);

  Single<TransactionV2> getTransactionByEventId(String eventId, Set<String> requestingParties);

  Single<TransactionV2> getTransactionByEventId(
      String eventId, Set<String> requestingParties, String accessToken);

  Single<TransactionV2> getTransactionById(String transactionId, Set<String> requestingParties);

  Single<TransactionV2> getTransactionById(
      String transactionId, Set<String> requestingParties, String accessToken);
}
