// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.CommandSubmissionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class SubmitRequest {

    static public CommandSubmissionServiceOuterClass.SubmitRequest toProto(@NonNull String ledgerId,
                                                                           @NonNull String workflowId,
                                                                           @NonNull String applicationId,
                                                                           @NonNull String commandId,
                                                                           @NonNull String party,
                                                                           @NonNull Optional<Instant> minLedgerTimeAbs,
                                                                           @NonNull Optional<Duration> minLedgerTimeRel,
                                                                           @NonNull Optional<Duration> deduplicationTime,
                                                                           @NonNull List<@NonNull Command> commands) {
        return CommandSubmissionServiceOuterClass.SubmitRequest.newBuilder()
                .setCommands(SubmitCommandsRequest.toProto(ledgerId, workflowId, applicationId, commandId,
                        party, minLedgerTimeAbs, minLedgerTimeRel, deduplicationTime, commands))
                .build();
    }

}
