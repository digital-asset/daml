// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.grpc.adapter.ExecutionSequencerFactory;
import com.daml.ledger.api.v1.CommandCompletionServiceGrpc;
import com.daml.ledger.api.v1.CommandCompletionServiceOuterClass;
import com.daml.ledger.javaapi.data.CompletionEndResponse;
import com.daml.ledger.javaapi.data.CompletionStreamRequest;
import com.daml.ledger.javaapi.data.CompletionStreamResponse;
import com.daml.ledger.javaapi.data.LedgerOffset;
import com.daml.ledger.rxjava.CommandCompletionClient;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import com.daml.ledger.rxjava.util.ClientPublisherFlowable;
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

  public CommandCompletionClientImpl(
      String ledgerId,
      Channel channel,
      ExecutionSequencerFactory sequencerFactory,
      Optional<String> accessToken) {
    this.ledgerId = ledgerId;
    this.sequencerFactory = sequencerFactory;
    this.serviceStub =
        StubHelper.authenticating(CommandCompletionServiceGrpc.newStub(channel), accessToken);
    this.serviceFutureStub =
        StubHelper.authenticating(CommandCompletionServiceGrpc.newFutureStub(channel), accessToken);
  }

  private Flowable<CompletionStreamResponse> completionStream(
      CompletionStreamRequest request, Optional<String> accessToken) {
    return ClientPublisherFlowable.create(
            request.toProto(),
            StubHelper.authenticating(serviceStub, accessToken)::completionStream,
            sequencerFactory)
        .map(CompletionStreamResponse::fromProto);
  }

  @Override
  public Flowable<CompletionStreamResponse> completionStream(
      String applicationId, LedgerOffset offset, Set<String> parties) {
    return completionStream(
        new CompletionStreamRequest(ledgerId, applicationId, parties, offset), Optional.empty());
  }

  @Override
  public Flowable<CompletionStreamResponse> completionStream(
      String applicationId, LedgerOffset offset, Set<String> parties, String accessToken) {
    return completionStream(
        new CompletionStreamRequest(ledgerId, applicationId, parties, offset),
        Optional.of(accessToken));
  }

  @Override
  public Flowable<CompletionStreamResponse> completionStream(
      String applicationId, Set<String> parties) {
    return completionStream(
        new CompletionStreamRequest(ledgerId, applicationId, parties), Optional.empty());
  }

  @Override
  public Flowable<CompletionStreamResponse> completionStream(
      String applicationId, Set<String> parties, String accessToken) {
    return completionStream(
        new CompletionStreamRequest(ledgerId, applicationId, parties), Optional.of(accessToken));
  }

  private Single<CompletionEndResponse> completionEnd(Optional<String> accessToken) {
    CommandCompletionServiceOuterClass.CompletionEndRequest request =
        CommandCompletionServiceOuterClass.CompletionEndRequest.newBuilder()
            .setLedgerId(ledgerId)
            .build();
    return Single.fromFuture(
            StubHelper.authenticating(serviceFutureStub, accessToken).completionEnd(request))
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
