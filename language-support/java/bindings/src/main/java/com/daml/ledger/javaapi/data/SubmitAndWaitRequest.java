// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.CommandServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class SubmitAndWaitRequest {

    public static CommandServiceOuterClass.SubmitAndWaitRequest toProto(@NonNull String ledgerId,
                                                                        @NonNull String workflowId,
                                                                        @NonNull String applicationId,
                                                                        @NonNull String commandId,
                                                                        @NonNull String party,
                                                                        @NonNull Optional<Instant> minLedgerTimeAbsolute,
                                                                        @NonNull Optional<Duration> minLedgerTimeRelative,
                                                                        @NonNull Optional<Duration> deduplicationTime,
                                                                        @NonNull List<@NonNull Command> commands) {
        return CommandServiceOuterClass.SubmitAndWaitRequest.newBuilder()
                .setCommands(SubmitCommandsRequest.toProto(ledgerId, workflowId, applicationId,
                        commandId, party, minLedgerTimeAbsolute, minLedgerTimeRelative, deduplicationTime, commands))
                .build();
    }
}
