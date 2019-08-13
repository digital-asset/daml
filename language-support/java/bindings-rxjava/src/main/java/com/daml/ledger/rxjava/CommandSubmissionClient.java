// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.Command;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import io.reactivex.Single;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.List;

/**
 * An RxJava version of {@link com.digitalasset.ledger.api.v1.CommandSubmissionServiceGrpc}
 */
public interface CommandSubmissionClient {

    Single<Empty> submit(@NonNull String workflowId,
                         @NonNull String applicationId,
                         @NonNull String commandId,
                         @NonNull String party,
                         @NonNull Instant ledgerEffectiveTime,
                         @NonNull Instant maximumRecordTime,
                         @NonNull List<@NonNull Command> commands);
}
