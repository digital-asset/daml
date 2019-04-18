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
import io.reactivex.Single;

import java.time.Instant;
import java.util.List;

public class CommandClientImpl implements CommandClient {

    private final String ledgerId;
    private final CommandServiceGrpc.CommandServiceFutureStub serviceStub;

    public CommandClientImpl(String ledgerId, Channel channel) {
        this.ledgerId = ledgerId;
        this.serviceStub = CommandServiceGrpc.newFutureStub(channel);
    }

    @Override
    public Single<Empty> submitAndWait(String workflowId, String applicationId,
                                       String commandId, String party, Instant ledgerEffectiveTime,
                                       Instant maximumRecordTime, List<Command> commands) {
        CommandServiceOuterClass.SubmitAndWaitRequest request = SubmitAndWaitRequest.toProto(this.ledgerId,
                workflowId, applicationId, commandId, party, ledgerEffectiveTime, maximumRecordTime, commands);
        return Single.fromFuture(serviceStub.submitAndWait(request));
    }
}
