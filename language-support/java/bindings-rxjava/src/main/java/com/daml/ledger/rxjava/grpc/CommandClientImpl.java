// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.ledger.rxjava.CommandClient;
import com.daml.ledger.javaapi.data.Command;
import com.daml.ledger.javaapi.data.SubmitAndWaitRequest;
import com.digitalasset.ledger.api.v1.CommandServiceGrpc;
import com.digitalasset.ledger.api.v1.CommandServiceOuterClass;
import com.google.protobuf.Empty;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.reactivex.Single;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.List;

public class CommandClientImpl implements CommandClient {

    private final String ledgerId;
    private final CommandServiceGrpc.CommandServiceFutureStub serviceStub;

    public CommandClientImpl(@NonNull String ledgerId, @NonNull Channel channel) {
        this.ledgerId = ledgerId;
        this.serviceStub = CommandServiceGrpc.newFutureStub(channel);
    }

    @Override
    public Single<Empty> submitAndWait(@NonNull String workflowId, @NonNull String applicationId,
                                       @NonNull String commandId, @NonNull String party, @NonNull Instant ledgerEffectiveTime,
                                       @NonNull Instant maximumRecordTime, @NonNull List<@NonNull Command> commands) {
        CommandServiceOuterClass.SubmitAndWaitRequest request = SubmitAndWaitRequest.toProto(this.ledgerId,
                workflowId, applicationId, commandId, party, ledgerEffectiveTime, maximumRecordTime, commands);
        return Single.fromFuture(serviceStub.submitAndWait(request));
    }
}
