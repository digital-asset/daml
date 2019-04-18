// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.ledger.rxjava.CommandSubmissionClient;
import com.daml.ledger.javaapi.data.Command;
import com.daml.ledger.javaapi.data.SubmitRequest;
import com.digitalasset.ledger.api.v1.CommandSubmissionServiceGrpc;
import com.digitalasset.ledger.api.v1.CommandSubmissionServiceOuterClass;
import io.grpc.Channel;
import io.reactivex.Single;

import java.time.Instant;
import java.util.List;

public class CommandSubmissionClientImpl implements CommandSubmissionClient {

    private final String ledgerId;
    private final CommandSubmissionServiceGrpc.CommandSubmissionServiceFutureStub serviceStub;

    public CommandSubmissionClientImpl(String ledgerId, Channel channel) {
        this.ledgerId = ledgerId;
        this.serviceStub = CommandSubmissionServiceGrpc.newFutureStub(channel);
    }

    @Override
    public Single<com.google.protobuf.Empty> submit(String workflowId,
                                                    String applicationId,
                                                    String commandId,
                                                    String party,
                                                    Instant ledgerEffectiveTime,
                                                    Instant maximumRecordTime,
                                                    List<Command> commands) {
        CommandSubmissionServiceOuterClass.SubmitRequest request = SubmitRequest.toProto(ledgerId,
                workflowId, applicationId, commandId, party, ledgerEffectiveTime, maximumRecordTime, commands);
        return Single
                .fromFuture(serviceStub.submit(request));
    }

}
