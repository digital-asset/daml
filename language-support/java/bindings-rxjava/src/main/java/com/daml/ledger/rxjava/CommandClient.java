// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.CommandsSubmissionV2;
import com.daml.ledger.javaapi.data.TransactionV2;
import com.daml.ledger.javaapi.data.TransactionTreeV2;
import com.daml.ledger.javaapi.data.UpdateSubmissionV2;
import com.google.protobuf.Empty;
import io.reactivex.Single;
import org.checkerframework.checker.nullness.qual.NonNull;

/** An RxJava version of {@link com.daml.ledger.api.v2.CommandServiceGrpc} */
public interface CommandClient {
  Single<Empty> submitAndWait(CommandsSubmissionV2 submission);

  Single<String> submitAndWaitForTransactionId(CommandsSubmissionV2 submission);

  Single<TransactionV2> submitAndWaitForTransaction(CommandsSubmissionV2 submission);

  Single<TransactionTreeV2> submitAndWaitForTransactionTree(CommandsSubmissionV2 submission);

  <U> Single<U> submitAndWaitForResult(@NonNull UpdateSubmissionV2<U> submission);
}
