// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.ledger.javaapi.data.Command;
import com.daml.ledger.javaapi.data.SubmitRequest;
import com.daml.ledger.rxjava.CommandSubmissionClient;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import com.daml.ledger.api.v1.CommandSubmissionServiceGrpc;
import com.daml.ledger.api.v1.CommandSubmissionServiceOuterClass;
import com.google.protobuf.Empty;
import io.grpc.Channel;
import io.reactivex.Single;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class CommandSubmissionClientImpl implements CommandSubmissionClient {

    private final String ledgerId;
    private final CommandSubmissionServiceGrpc.CommandSubmissionServiceFutureStub serviceStub;

    public CommandSubmissionClientImpl(@NonNull String ledgerId, @NonNull Channel channel, Optional<String> accessToken) {
        this.ledgerId = ledgerId;
        this.serviceStub = StubHelper.authenticating(CommandSubmissionServiceGrpc.newFutureStub(channel), accessToken);
    }

    public Single<com.google.protobuf.Empty> submit(@NonNull String workflowId,
                                                    @NonNull String applicationId,
                                                    @NonNull String commandId,
                                                    @NonNull String party,
                                                    @NonNull Optional<Instant> minLedgerTimeAbs,
                                                    @NonNull Optional<Duration> minLedgerTimeRel,
                                                    @NonNull Optional<Duration> deduplicationTime,
                                                    @NonNull List<@NonNull Command> commands,
                                                    Optional<String> accessToken) {
        CommandSubmissionServiceOuterClass.SubmitRequest request = SubmitRequest.toProto(ledgerId,
                workflowId, applicationId, commandId, party, minLedgerTimeAbs, minLedgerTimeRel, deduplicationTime, commands);
        return Single
                .fromFuture(StubHelper.authenticating(this.serviceStub, accessToken).submit(request));
    }

    @Override
    public Single<com.google.protobuf.Empty> submit(@NonNull String workflowId,
                                                    @NonNull String applicationId,
                                                    @NonNull String commandId,
                                                    @NonNull String party,
                                                    @NonNull List<@NonNull Command> commands) {
        return submit(workflowId, applicationId, commandId, party, Optional.empty(), Optional.empty(), Optional.empty(), commands, Optional.empty());
    }

    @Override
    public Single<com.google.protobuf.Empty> submit(@NonNull String workflowId,
                                                    @NonNull String applicationId,
                                                    @NonNull String commandId,
                                                    @NonNull String party,
                                                    @NonNull Optional<Instant> minLedgerTimeAbs,
                                                    @NonNull Optional<Duration> minLedgerTimeRel,
                                                    @NonNull Optional<Duration> deduplicationTime,
                                                    @NonNull List<@NonNull Command> commands,
                                                    @NonNull String accessToken) {
        return submit(workflowId, applicationId, commandId, party, minLedgerTimeAbs, minLedgerTimeRel, deduplicationTime, commands, Optional.of(accessToken));
    }

    @Override
    public Single<Empty> submit(@NonNull String workflowId,
                                @NonNull String applicationId,
                                @NonNull String commandId,
                                @NonNull String party,
                                @NonNull Optional<Instant> minLedgerTimeAbs,
                                @NonNull Optional<Duration> minLedgerTimeRel,
                                @NonNull Optional<Duration> deduplicationTime,
                                @NonNull List<@NonNull Command> commands) {
        return submit(workflowId, applicationId, commandId, party, minLedgerTimeAbs, minLedgerTimeRel, deduplicationTime, commands, Optional.empty());
    }

    @Override
    public Single<Empty> submit(@NonNull String workflowId,
                                @NonNull String applicationId,
                                @NonNull String commandId,
                                @NonNull String party,
                                @NonNull List<@NonNull Command> commands,
                                @NonNull String accessToken) {
        return submit(workflowId, applicationId, commandId, party, Optional.empty(), Optional.empty(), Optional.empty(), commands, Optional.of(accessToken));
    }
}
