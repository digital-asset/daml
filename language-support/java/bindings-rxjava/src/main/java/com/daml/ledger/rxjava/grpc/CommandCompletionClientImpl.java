// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.ledger.rxjava.CommandCompletionClient;
import com.daml.ledger.javaapi.data.CompletionEndResponse;
import com.daml.ledger.javaapi.data.CompletionStreamRequest;
import com.daml.ledger.javaapi.data.CompletionStreamResponse;
import com.daml.ledger.javaapi.data.LedgerOffset;
import com.daml.ledger.rxjava.util.ClientPublisherFlowable;
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory;
import com.digitalasset.ledger.api.v1.CommandCompletionServiceGrpc;
import com.digitalasset.ledger.api.v1.CommandCompletionServiceOuterClass;
import io.grpc.Channel;
import io.reactivex.Flowable;
import io.reactivex.Single;

import java.util.Set;

public class CommandCompletionClientImpl implements CommandCompletionClient {

    private final String ledgerId;
    private final CommandCompletionServiceGrpc.CommandCompletionServiceStub serviceStub;
    private final CommandCompletionServiceGrpc.CommandCompletionServiceFutureStub serviceFutureStub;
    private final ExecutionSequencerFactory sequencerFactory;

    public CommandCompletionClientImpl(String ledgerId, Channel channel, ExecutionSequencerFactory sequencerFactory) {
        this.ledgerId = ledgerId;
        this.sequencerFactory = sequencerFactory;
        serviceStub = CommandCompletionServiceGrpc.newStub(channel);
        serviceFutureStub = CommandCompletionServiceGrpc.newFutureStub(channel);
    }

    @Override
    public Flowable<CompletionStreamResponse> completionStream(String applicationId, LedgerOffset offset, Set<String> parties) {
        CommandCompletionServiceOuterClass.CompletionStreamRequest request = new CompletionStreamRequest(ledgerId, applicationId, parties, offset).toProto();
        return ClientPublisherFlowable
                .create(request, serviceStub::completionStream, sequencerFactory)
                .map(CompletionStreamResponse::fromProto);
    }

    @Override
    public Single<CompletionEndResponse> completionEnd() {
        CommandCompletionServiceOuterClass.CompletionEndRequest request = CommandCompletionServiceOuterClass.CompletionEndRequest.newBuilder().setLedgerId(ledgerId).build();
        return Single
                .fromFuture(serviceFutureStub.completionEnd(request))
                .map(CompletionEndResponse::fromProto);
    }
}
