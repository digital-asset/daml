// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.CommandsSubmission;
import com.daml.ledger.javaapi.data.Transaction;
import com.daml.ledger.javaapi.data.TransactionTree;
import com.daml.ledger.javaapi.data.UpdateSubmission;
import com.google.protobuf.Empty;
import io.reactivex.Single;
import org.checkerframework.checker.nullness.qual.NonNull;

/** An RxJava version of {@link com.daml.ledger.api.v2.CommandServiceGrpc} */
public interface CommandClient {
  Single<Empty> submitAndWait(CommandsSubmission submission);

  Single<String> submitAndWaitForTransactionId(CommandsSubmission submission);

  Single<Transaction> submitAndWaitForTransaction(CommandsSubmission submission);

  Single<TransactionTree> submitAndWaitForTransactionTree(CommandsSubmission submission);

  <U> Single<U> submitAndWaitForResult(@NonNull UpdateSubmission<U> submission);
}
