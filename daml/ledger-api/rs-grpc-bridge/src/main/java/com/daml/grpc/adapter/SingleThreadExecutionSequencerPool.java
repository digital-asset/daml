// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

public class SingleThreadExecutionSequencerPool implements ExecutionSequencerFactory {
  private final SingleThreadExecutionSequencer[] sequencers;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicInteger counter = new AtomicInteger(0);

  public SingleThreadExecutionSequencerPool(@Nonnull String poolName) {
    this(poolName, Runtime.getRuntime().availableProcessors());
  }

  public SingleThreadExecutionSequencerPool(@Nonnull String poolName, int instanceCount) {
    this.sequencers = new SingleThreadExecutionSequencer[instanceCount];
    for (int i = 0; i < instanceCount; i++) {
      sequencers[i] = new SingleThreadExecutionSequencer(poolName + "-" + i);
    }
    running.set(true);
  }

  @Override
  public ExecutionSequencer getExecutionSequencer() {
    if (!running.get()) {
      throw new IllegalStateException("The execution sequencer pool is not running.");
    }
    return sequencers[counter.getAndIncrement() % sequencers.length];
  }

  @Override
  public void close() throws Exception {
    if (running.compareAndSet(true, false)) {
      for (int i = 0; i < sequencers.length; i++) {
        sequencers[i].close();
        sequencers[i] = null;
      }
    }
  }
}
