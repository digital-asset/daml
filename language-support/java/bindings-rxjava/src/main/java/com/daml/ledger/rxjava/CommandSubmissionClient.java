// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.Command;
import com.google.protobuf.Empty;
import io.reactivex.Single;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * An RxJava version of {@link com.daml.ledger.api.v1.CommandSubmissionServiceGrpc}
 */
public interface CommandSubmissionClient {

    Single<Empty> submit(@NonNull String workflowId,
                         @NonNull String applicationId,
                         @NonNull String commandId,
                         @NonNull String party,
                         @NonNull Optional<Instant> minLedgerTimeAbs,
                         @NonNull Optional<Duration> minLedgerTimeRel,
                         @NonNull Optional<Duration> deduplicationTime,
                         @NonNull List<@NonNull Command> commands);

    Single<Empty> submit(@NonNull String workflowId,
                         @NonNull String applicationId,
                         @NonNull String commandId,
                         @NonNull String party,
                         @NonNull Optional<Instant> minLedgerTimeAbs,
                         @NonNull Optional<Duration> minLedgerTimeRel,
                         @NonNull Optional<Duration> deduplicationTime,
                         @NonNull List<@NonNull Command> commands,
                         @NonNull String accessToken);

    Single<Empty> submit(@NonNull String workflowId,
                         @NonNull String applicationId,
                         @NonNull String commandId,
                         @NonNull String party,
                         @NonNull List<@NonNull Command> commands);

    Single<Empty> submit(@NonNull String workflowId,
                         @NonNull String applicationId,
                         @NonNull String commandId,
                         @NonNull String party,
                         @NonNull List<@NonNull Command> commands,
                         @NonNull String accessToken);

}
