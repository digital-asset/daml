// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

// TODO(#24401) add fromProto and constructors
public final class SubmitAndWaitForTransactionRequest {

  public static CommandServiceOuterClass.SubmitAndWaitForTransactionRequest toProto(
      @NonNull CommandsSubmission submission, @NonNull TransactionFormat transactionFormat) {
    return CommandServiceOuterClass.SubmitAndWaitForTransactionRequest.newBuilder()
        .setCommands(submission.toProto())
        .setTransactionFormat(transactionFormat.toProto())
        .build();
  }
}
