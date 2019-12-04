// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.ledger.rxjava.CommandCompletionClient;
import com.daml.ledger.javaapi.data.CompletionEndResponse;
import com.daml.ledger.javaapi.data.CompletionStreamRequest;
import com.daml.ledger.javaapi.data.CompletionStreamResponse;
import com.daml.ledger.javaapi.data.LedgerOffset;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import com.daml.ledger.rxjava.util.ClientPublisherFlowable;
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory;
import com.digitalasset.ledger.api.v1.CommandCompletionServiceGrpc;
import com.digitalasset.ledger.api.v1.CommandCompletionServiceOuterClass;
import io.grpc.Channel;
import io.reactivex.Flowable;
import io.reactivex.Single;

import java.util.Optional;
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

    private Flowable<CompletionStreamResponse> completionStream(CompletionStreamRequest request, Optional<String> accessToken) {
        return ClientPublisherFlowable
                .create(request.toProto(), StubHelper.authenticating(serviceStub, accessToken)::completionStream, sequencerFactory)
                .map(CompletionStreamResponse::fromProto);
    }

    @Override
    public Flowable<CompletionStreamResponse> completionStream(String applicationId, LedgerOffset offset, Set<String> parties) {
        return completionStream(new CompletionStreamRequest(ledgerId, applicationId, parties, offset), Optional.empty());
    }

    @Override
    public Flowable<CompletionStreamResponse> completionStream(String applicationId, LedgerOffset offset, Set<String> parties, String accessToken) {
        return completionStream(new CompletionStreamRequest(ledgerId, applicationId, parties, offset), Optional.of(accessToken));
    }

    @Override
    public Flowable<CompletionStreamResponse> completionStream(String applicationId, Set<String> parties) {
        return completionStream(new CompletionStreamRequest(ledgerId, applicationId, parties), Optional.empty());
    }

    @Override
    public Flowable<CompletionStreamResponse> completionStream(String applicationId, Set<String> parties, String accessToken) {
        return completionStream(new CompletionStreamRequest(ledgerId, applicationId, parties), Optional.of(accessToken));
    }

    private Single<CompletionEndResponse> completionEnd(Optional<String> accessToken) {
        CommandCompletionServiceOuterClass.CompletionEndRequest request = CommandCompletionServiceOuterClass.CompletionEndRequest.newBuilder().setLedgerId(ledgerId).build();
        return Single
                .fromFuture(StubHelper.authenticating(serviceFutureStub, accessToken).completionEnd(request))
                .map(CompletionEndResponse::fromProto);
    }

    @Override
    public Single<CompletionEndResponse> completionEnd() {
        return completionEnd(Optional.empty());
    }

    @Override
    public Single<CompletionEndResponse> completionEnd(String accessToken) {
        return completionEnd(Optional.of(accessToken));
    }
}
