// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.client.rs;

import com.daml.grpc.adapter.ExecutionSequencerFactory;
import io.grpc.stub.StreamObserver;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * RS-gRPC bridge component for server streaming. Defers execution of the RPC call until a
 * subscription is made. Supports multiple subscribers. Each subscription will trigger executing the
 * same RPC call.
 *
 * @param <Req> The request type of gRPC call.
 * @param <Resp> The response type of the gRPC call.
 */
public class ClientPublisher<Req, Resp> implements Publisher<Resp> {

  @Nonnull private final Req request;
  @Nonnull private final BiConsumer<Req, StreamObserver<Resp>> clientStub;
  @Nonnull private final ExecutionSequencerFactory esf;

  public ClientPublisher(
      @Nonnull Req request,
      @Nonnull BiConsumer<Req, StreamObserver<Resp>> clientStub,
      @Nonnull ExecutionSequencerFactory esf) {
    this.request = request;
    this.clientStub = clientStub;
    this.esf = esf;
  }

  /**
   * Executes the RPC call right away and feeds its results to the subscriber on demand.
   *
   * @param subscriber The target subscriber.
   */
  @Override
  public void subscribe(Subscriber<? super Resp> subscriber) {

    if (subscriber == null)
      throw new NullPointerException("RS downstream called onSubscribe with null subscriber.");
    else {
      this.clientStub.accept(
          request,
          new BufferingResponseObserver<Req, Resp>(subscriber, esf.getExecutionSequencer()));
    }
  }
}
