// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.client.rs;

import com.daml.grpc.adapter.CallCounter;
import com.daml.grpc.adapter.ExecutionSequencer;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BufferingResponseObserver<Req, Resp> implements ClientResponseObserver<Req, Resp> {

  private static final Logger logger = LoggerFactory.getLogger(BufferingResponseObserver.class);

  @Nonnull private BufferingSubscription<Resp> subscription;
  @Nonnull private final ExecutionSequencer es;
  @Nonnull private final String logPrefix = String.format("Call %d: ", CallCounter.getNewCallId());
  @Nonnull private final UpstreamEventBuffer<Resp> buffer = new UpstreamEventBuffer<>(logPrefix);

  /** Null until call is executed. */
  @Nullable private ClientCallStreamObserver<Req> requestObserver;

  BufferingResponseObserver(
      @Nonnull Subscriber<? super Resp> subscriber, @Nonnull ExecutionSequencer es) {
    this.es = es;
    this.subscription =
        new BufferingSubscription<Resp>(
            buffer::flushOnFirstRequest, () -> requestObserver, this.es, subscriber, logPrefix);
    subscriber.onSubscribe(subscription);
    logger.trace("{}RS downstream subscription registered.", logPrefix);
  }

  @Override
  public void beforeStart(ClientCallStreamObserver<Req> ccso) {
    // IMPORTANT: methods of the ClientCallStreamObserver are not safe to call here or after this
    // method returns.
    // We need to wait for the first upstream signal (onNext/onComplete/onError) to know for sure
    // that the call
    // has already been started. Failing to do so may result in IllegalStateExceptions thrown by the
    // CCSO.
    logger.trace("{}Starting call.", logPrefix);
    ccso.disableAutoInboundFlowControl();
    es.sequence(() -> requestObserver = ccso);
  }

  @Override
  public void onNext(Resp response) {
    logger.trace("{}gRPC upstream emitted response message {}.", logPrefix, response);
    es.sequence(
        () -> {
          subscription.onNextElement(requestObserver);
          if (!subscription.isCancelled()) {
            // Buffer any elements while the first request call was not made.
            if (!subscription.isFirstRequestPending()) {
              subscription.getSubscriber().onNext(response);
            } else {
              boolean bufferingSucceeded = buffer.onNext(response);
              if (!bufferingSucceeded) {
                // Upstream violated the contract, so we cancel it.
                // Downstream will be notified of the failure when the buffer is flushed.
                subscription.cancel();
              }
            }
          }
        });
  }

  @Override
  public void onError(Throwable throwable) {
    logger.trace("{}gRPC upstream emitted error.", logPrefix, throwable);
    es.sequence(
        () -> {
          if (!subscription.isCancelled()) {
            if (buffer.hasNoElement()) {
              subscription.getSubscriber().onError(throwable);
            } else {
              buffer.onError(throwable);
            }
          }
        });
  }

  @Override
  public void onCompleted() {
    logger.trace("{}gRPC upstream completed.", logPrefix);
    es.sequence(
        () -> {
          if (!subscription.isCancelled()) {
            if (buffer.hasNoElement()) {
              subscription.getSubscriber().onComplete();
            } else {
              buffer.onComplete();
            }
          }
        });
  }
}
