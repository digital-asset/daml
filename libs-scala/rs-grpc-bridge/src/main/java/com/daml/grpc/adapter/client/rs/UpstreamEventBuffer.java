// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.client.rs;

import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is not thread safe, synchronization must be handled outside of it.
 *
 * <p>Buffers events that happen upstream of a ClientPublisher component, including: - a single
 * element - stream completion - stream failure. These events can then be flushed to a downstream
 * subscriber via the appropriate flush methods.
 *
 * @param <Elem> The type of element this class can buffer.
 */
class UpstreamEventBuffer<Elem> {

  private static Logger logger = LoggerFactory.getLogger(UpstreamEventBuffer.class);

  @Nullable private Elem bufferedElem;

  /**
   * Null if no completion received. Empty if completed successfully. Non-empty with Thowable if
   * completed with error.
   */
  @Nullable private Optional<Throwable> completion;

  @Nonnull private final String logPrefix;

  UpstreamEventBuffer(@Nonnull String logPrefix) {
    this.logPrefix = logPrefix;
  }

  /**
   * @return true if the elem could be buffered. If false, the upstream gRPC connection should be
   *     canceled, since it violated the contract and pushed 2 elements without demand. The
   *     downstream will receive an error about this when flushed.
   */
  boolean onNext(@Nonnull Elem response) {
    if (bufferedElem != null) {
      // We use this roundabout way to signal the error, because if we'd just throw it,
      // it would just get logged by the ExecutionSequencer without any other effects.
      onError(new IllegalStateException("gRPC upstream pushed 2 elements without demand."));
      return false;
    } else {
      logger.trace("{}Message {} was buffered.", logPrefix, response);
      bufferedElem = response;
      return true;
    }
  }

  void onComplete() {
    if (completion != null) {
      if (completion.isPresent()) {
        Throwable bufferedError = completion.get();
        completion =
            Optional.of(
                new IllegalStateException(
                    "gRPC upstream signaled completion after error.", bufferedError));
      } else {
        completion =
            Optional.of(new IllegalStateException("gRPC upstream signaled completion twice."));
      }
    } else {
      logger.trace("{}Successful completion was buffered.", logPrefix);
      completion = Optional.empty();
    }
  }

  void onError(@Nonnull Throwable throwable) {
    if (completion != null) {
      if (completion.isPresent()) {
        IllegalStateException moreThanOneError =
            new IllegalStateException("gRPC upstream signaled multiple errors.", completion.get());
        moreThanOneError.addSuppressed(throwable);
        completion = Optional.of(moreThanOneError);
      } else {
        completion =
            Optional.of(
                new IllegalStateException(
                    "gRPC upstream signaled error after completion.", throwable));
      }
    } else {
      logger.trace("{}Stream failure buffered", logPrefix, throwable);
      completion = Optional.of(throwable);
    }
  }

  /** To be called on the first request() call to flush buffered state. */
  void flushOnFirstRequest(@Nonnull Subscriber<? super Elem> subscriber) {
    flushBufferedElem(subscriber);
    flushCompletion(subscriber);
  }

  private void flushBufferedElem(@Nonnull Subscriber<? super Elem> subscriber) {
    if (bufferedElem != null) {
      logger.trace("{}Flushing buffered element {} to RS downstream.", logPrefix, bufferedElem);
      subscriber.onNext(bufferedElem);
      bufferedElem = null;
    }
  }

  private void flushCompletion(@Nonnull Subscriber subscriber) {
    if (completion != null) {
      logger.trace("{}Flushing completion to RS downstream.", logPrefix);
      if (completion.isPresent()) subscriber.onError(completion.get());
      else subscriber.onComplete();
      completion = null;
    }
  }

  /** @return true if no elem was buffered until now or it has already been flushed. */
  boolean hasNoElement() {
    return bufferedElem == null;
  }
}
