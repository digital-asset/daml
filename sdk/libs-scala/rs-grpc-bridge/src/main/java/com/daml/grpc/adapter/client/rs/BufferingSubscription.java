// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.client.rs;

import com.daml.grpc.adapter.ExecutionSequencer;
import io.grpc.stub.ClientCallStreamObserver;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used in the ClientPublisher to handle downstream signals by either buffering them or forwarding
 * them to an appropriate ClientCallStreamObserver.
 */
class BufferingSubscription<Resp> implements Subscription {

  private static final Logger logger = LoggerFactory.getLogger(BufferingSubscription.class);

  // Immutable fields. See constructor for docs.

  @Nonnull private final Consumer<Subscriber<? super Resp>> onFirstDemand;
  @Nonnull private final Supplier<ClientCallStreamObserver> requestObserverSupplier;
  @Nonnull private final ExecutionSequencer es;
  @Nonnull private final Subscriber<? super Resp> subscriber;
  @Nonnull private final String logPrefix;
  @Nonnull private final DownstreamEventBuffer downstreamEventBuffer;

  private boolean hasReceivedElements = false;

  /**
   * gRPC upstream will send an element that the subscriber did not ask for. We keep track of that,
   * and any new demand that was signaled to the transport layer in this variable. It gets
   * decremented with each incoming element, and once it reaches zero, part of the buffered
   * downstream demand (capped at Integer.MAX_VALUE) will be flushed upstream, and added to this
   * counter.
   */
  @Nonnegative private int unsatisfiedDemand = 1;

  // Mutable fields

  /**
   * Set on downstream cancellation. Once this is true, no elements should be emitted to downstream.
   */
  private boolean cancelled = false;

  /** True until the first request() call is made to this object */
  private boolean firstRequestPending = true;

  /**
   * @param onFirstDemand Will be called when the downstream signals its first demand.
   *     Implementation should flush any buffered elements and completion signals to the subscriber.
   * @param requestObserverSupplier Should return the ClientCallStreamObserver if the request was
   *     already started, or null otherwise.
   * @param es Will be used to sequence all callbacks that are passed and all operations that would
   *     change mutable state.
   * @param subscriber The subscriber this subscription was created for.
   * @param logPrefix
   */
  BufferingSubscription(
      @Nonnull Consumer<Subscriber<? super Resp>> onFirstDemand,
      @Nonnull Supplier<ClientCallStreamObserver> requestObserverSupplier,
      @Nonnull ExecutionSequencer es,
      @Nonnull Subscriber<? super Resp> subscriber,
      @Nonnull String logPrefix) {
    this.onFirstDemand = onFirstDemand;
    this.requestObserverSupplier = requestObserverSupplier;
    this.es = es;
    this.subscriber = subscriber;
    this.logPrefix = logPrefix;
    downstreamEventBuffer = new DownstreamEventBuffer(this::propagateIntegerDemand, logPrefix);
  }

  // Getters

  @Nonnull
  public Subscriber<? super Resp> getSubscriber() {
    return subscriber;
  }

  boolean isCancelled() {
    return cancelled;
  }

  boolean isFirstRequestPending() {
    return firstRequestPending;
  }

  // Actual business logic

  @Override
  public void request(long demand) {
    logger.trace("{}RS downstream signaled demand for {} elements.", logPrefix, demand);
    if (demand < 1) {
      terminateStream(
          String.format("Subscriber signaled non-positive subscription request (%d)", demand));
    } else {
      es.sequence(
          () -> {
            if (!cancelled) {
              accumulateDemand(demand);
              propagateDemandIfNoElemsPending();
            } else {
              logger.trace(
                  "{}Swallowing demand from gRPC upstream due to previous cancellation.",
                  logPrefix);
            }
          });
    }
  }

  private void propagateDemandIfNoElemsPending() {
    ClientCallStreamObserver clientCallStreamObserver = requestObserverSupplier.get();
    if (clientCallStreamObserver != null && hasReceivedElements && unsatisfiedDemand == 0) {
      this.unsatisfiedDemand = downstreamEventBuffer.propagateDemand(clientCallStreamObserver);
    }
  }

  private void terminateStream(String cause) {
    subscriber.onError(new IllegalArgumentException(cause));
    cancel();
  }

  private void accumulateDemand(long demand) {
    if (firstRequestPending) {
      firstRequestPending = false;
      onFirstDemand.accept(subscriber);
      downstreamEventBuffer.bufferDemand(demand - 1L);
    } else {
      downstreamEventBuffer.bufferDemand(demand);
    }
  }

  private void propagateIntegerDemand(
      @Nonnull ClientCallStreamObserver requestObserver, @Nonnegative int demand) {
    requestObserver.request(demand);
  }

  @Override
  public void cancel() {
    logger.trace("{}RS downstream signaled cancellation.", logPrefix);
    es.sequence(
        () -> {
          cancelled = true;
          ClientCallStreamObserver requestObserver = requestObserverSupplier.get();
          if (requestObserver != null) propagateCancellation(requestObserver);
        });
  }

  private void propagateCancellation(@Nonnull ClientCallStreamObserver requestObserver) {
    try {
      requestObserver.cancel("Client cancelled the call.", null);
    } catch (IllegalStateException e) {
      logger.warn("Failed to propagate cancellation.", e);
    }
  }

  /**
   * If there's no unsatisfied demand signaled to the upstream, it sends the buffered amount
   *
   * @param requestObserver Used to signal demand or cancellation to the gRPC upstream if necessary.
   */
  void onNextElement(@Nonnull ClientCallStreamObserver requestObserver) {
    hasReceivedElements = true;
    if (cancelled) {
      propagateCancellation(requestObserver);
    } else if (--unsatisfiedDemand == 0) {
      this.unsatisfiedDemand = downstreamEventBuffer.propagateDemand(requestObserver);
    }
  }
}
