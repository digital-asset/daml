// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.grpc.adapter.ExecutionSequencerFactory;
import com.daml.ledger.api.v2.CommandCompletionServiceGrpc;
import com.daml.ledger.javaapi.data.CompletionStreamRequest;
import com.daml.ledger.javaapi.data.CompletionStreamResponse;
import com.daml.ledger.javaapi.data.ParticipantOffset;
import com.daml.ledger.rxjava.CommandCompletionClient;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import com.daml.ledger.rxjava.util.ClientPublisherFlowable;
import io.grpc.Channel;
import io.reactivex.Flowable;
import java.util.List;
import java.util.Optional;

public class CommandCompletionClientImpl implements CommandCompletionClient {

  private final CommandCompletionServiceGrpc.CommandCompletionServiceStub serviceStub;
  private final CommandCompletionServiceGrpc.CommandCompletionServiceFutureStub serviceFutureStub;
  private final ExecutionSequencerFactory sequencerFactory;

  public CommandCompletionClientImpl(
      Channel channel, ExecutionSequencerFactory sequencerFactory, Optional<String> accessToken) {
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
      String applicationId, ParticipantOffset offset, List<String> parties) {
    return completionStream(
        new CompletionStreamRequest(applicationId, parties, offset), Optional.empty());
  }

  @Override
  public Flowable<CompletionStreamResponse> completionStream(
      String applicationId, ParticipantOffset offset, List<String> parties, String accessToken) {
    return completionStream(
        new CompletionStreamRequest(applicationId, parties, offset), Optional.of(accessToken));
  }

  @Override
  public Flowable<CompletionStreamResponse> completionStream(
      String applicationId, List<String> parties) {
    return completionStream(
        new CompletionStreamRequest(
            applicationId, parties, ParticipantOffset.ParticipantBegin.getInstance()),
        Optional.empty());
  }

  @Override
  public Flowable<CompletionStreamResponse> completionStream(
      String applicationId, List<String> parties, String accessToken) {
    return completionStream(
        new CompletionStreamRequest(
            applicationId, parties, ParticipantOffset.ParticipantBegin.getInstance()),
        Optional.of(accessToken));
  }
}
