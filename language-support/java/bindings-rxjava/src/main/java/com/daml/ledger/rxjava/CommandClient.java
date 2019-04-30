// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.Command;
import com.daml.ledger.javaapi.data.TransactionTree;
import com.google.protobuf.Empty;
import io.reactivex.Maybe;
import io.reactivex.Single;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.List;

/**
 * An RxJava version of {@link com.digitalasset.ledger.api.v1.CommandServiceGrpc}
 */
public interface CommandClient {

    /**
     * @return an empty or non-empty {@link Maybe} if the command was processed successfully,
     * or an error otherwise. The {@link Maybe} contains a value if the response from the
     * ledger contained a transaction tree.
     */
    Maybe<TransactionTree> submitAndWait(@NonNull String workflowId, @NonNull String applicationId,
                                         @NonNull String commandId, @NonNull String party, @NonNull Instant ledgerEffectiveTime,
                                         @NonNull Instant maximumRecordTime, @NonNull List<@NonNull Command> commands);
}
