// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.server.rs;

import com.daml.grpc.adapter.CallCounter;
import com.daml.grpc.adapter.ExecutionSequencer;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerSubscriber<Resp> implements Subscriber<Resp> {

  private static final Logger logger = LoggerFactory.getLogger(ServerSubscriber.class);

  @Nonnull private final ServerCallStreamObserver<Resp> responseObserver;
  @Nonnull private final CompletableFuture<Void> completionPromise = new CompletableFuture<>();
  @Nonnull private final ExecutionSequencer executionSequencer;
  @Nonnull private final String logPrefix = String.format("Call %d: ", CallCounter.getNewCallId());

  private final BufferingEventHandler onReadyHandler =
      new BufferingEventHandler(
          subscription ->
              () -> {
                logger.trace(
                    "{}gRPC downstream is ready. Demanding new response from RS upstream.",
                    logPrefix);
                subscription.request(1L);
              },
          "demand");

  private final BufferingEventHandler onCancelHandler =
      new BufferingEventHandler(
          subscription ->
              () -> {
                logger.trace("{}gRPC downstream canceled.", logPrefix);
                subscription.cancel();
                completionPromise.complete(null);
              },
          "cancellation");

  /** MUST be accessed via the executionSequencer */
  private boolean subscribed = false;

  public ServerSubscriber(
      @Nonnull ServerCallStreamObserver<Resp> responseObserver,
      @Nonnull ExecutionSequencer executionSequencer) {

    this.responseObserver = responseObserver;
    this.executionSequencer = executionSequencer;

    responseObserver.disableAutoInboundFlowControl();
    // These handlers will buffer demand and cancellation until the subscription arrives,
    // then propagate them properly after onSubscribe is called.
    responseObserver.setOnReadyHandler(onReadyHandler);
    responseObserver.setOnCancelHandler(onCancelHandler);
  }

  public CompletableFuture<Void> completionFuture = completionPromise;

  @Override
  public void onSubscribe(Subscription subscription) {
    if (subscription == null)
      throw new NullPointerException("RS upstream called onSubscribe with null argument.");
    logger.trace(
        "{}RS upstream subscription registered. Setting up onCancelHandler and onReadyHandler"
            + " handlers.",
        logPrefix);
    executionSequencer.sequence(
        () -> {
          if (!subscribed) {
            // Flush any buffered events to the subscription,
            // and use it directly to signal demand and cancellation from this point on.
            onCancelHandler.onSubscribe(subscription);
            onReadyHandler.onSubscribe(subscription);
            subscribed = true;
          } else {
            subscription.cancel();
          }
        });
  }

  @Override
  public void onNext(Resp response) {
    if (response == null)
      throw new NullPointerException("RS upstream called onNext with null argument.");
    logger.trace("{}RS upstream emitted response message {}.", logPrefix, response);
    executionSequencer.sequence(
        () -> {
          if (!responseObserver.isCancelled()) {
            synchronized (response) {
              // Ensure memory visibility of precomputation of message's serializedSize
              responseObserver.onNext(response);
            }
            if (responseObserver.isReady()) {
              onReadyHandler.run();
            } else {
              logger.trace(
                  "{}Backpressuring as gRPC downstream is not ready for next element.", logPrefix);
            }
          }
        });
  }

  @Override
  public void onError(Throwable throwable) {
    if (throwable == null)
      throw new NullPointerException("RS upstream called onError with null argument.");
    logger.trace("{}RS upstream failed.", logPrefix, throwable);
    executionSequencer.sequence(
        () -> {
          if (!responseObserver.isCancelled()) {
            Throwable newThrowable = translateThrowableInOnError(throwable);
            responseObserver.onError(newThrowable);
            completionPromise.completeExceptionally(newThrowable);
          }
        });
  }

  @Override
  public void onComplete() {
    logger.trace("{}RS upstream completed.", logPrefix);
    executionSequencer.sequence(
        () -> {
          if (!responseObserver.isCancelled()) {
            responseObserver.onCompleted();
            completionPromise.complete(null);
          }
        });
  }

  protected Throwable translateThrowableInOnError(Throwable throwable) {
    return throwable;
  }

  private class BufferingEventHandler implements Runnable {

    /**
     * True if an event was buffered before subscription, and has not been flushed yet. Not volatile
     * as it's always accessed from the executionSequencer
     */
    private boolean eventBuffered = false;

    @Nonnull private final Function<Subscription, Runnable> getPropagatingEventHandler;

    /** This is the real eventHandler, assigned when the subscription is passed in. */
    @Nullable private Runnable propagatingEventHandler;

    @Nonnull private final String eventKind;

    // Needs to be volatile as it is accessed from gRPC executors in run().
    @Nonnull private volatile Runnable currentEventHandler;

    @Override
    public void run() {
      currentEventHandler.run();
    }

    /**
     * @param getPropagatingEventHandler Will be called on subscription to create the eventHandler
     *     which can propagate demand and cancellation upstream. This eventHandler will run on the
     *     dedicated bridge thread.
     * @param eventKind The type of event this Runnable handles.
     */
    BufferingEventHandler(
        @Nonnull Function<Subscription, Runnable> getPropagatingEventHandler,
        @Nonnull String eventKind) {
      this.getPropagatingEventHandler = getPropagatingEventHandler;
      this.eventKind = eventKind;
      // This eventHandler is capable of buffering a single event, and will be replaced on the call
      // to onSubscribe.
      this.currentEventHandler =
          () ->
              executionSequencer.sequence(
                  () -> {
                    if (propagatingEventHandler == null) {
                      bufferEvent();
                    } else {
                      // By the time this runnable was scheduled, the subscription was passed in.
                      // We can use it to propagate the event.
                      propagatingEventHandler.run();
                    }
                  });
    }

    private void bufferEvent() {
      // We *could* report an error if something gets buffered twice before the subscription,
      // but the events that are handled here are idempotent, so there's no need to do so.
      logger.trace("{}Buffered {} event from gRPC downstream.", logPrefix, eventKind);
      eventBuffered = true;
    }

    /** MUST be called from the executionSequencer. */
    void onSubscribe(@Nonnull Subscription subscription) {
      propagatingEventHandler = getPropagatingEventHandler.apply(subscription);
      currentEventHandler = () -> executionSequencer.sequence(propagatingEventHandler);
      if (eventBuffered) {
        logger.trace("{}Flushing {} event to RS upstream.", logPrefix, eventKind);
        propagatingEventHandler.run();
        eventBuffered = false;
      }
    }
  }
}
