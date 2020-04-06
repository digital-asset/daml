// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicInteger;

public class SingleThreadExecutionSequencerPool implements ExecutionSequencerFactory{

    private final int instanceCount;
    @Nonnull
    private final SingleThreadExecutionSequencer[] sequencers;

    private final AtomicInteger counter = new AtomicInteger(0);

    public SingleThreadExecutionSequencerPool(@Nonnull String poolName) {
        this.instanceCount = Runtime.getRuntime().availableProcessors();
        this.sequencers = new SingleThreadExecutionSequencer[instanceCount];
        initPool(poolName);
    }

    public SingleThreadExecutionSequencerPool(@Nonnull String poolName, int instanceCount) {
        this.instanceCount = instanceCount;
        this.sequencers = new SingleThreadExecutionSequencer[instanceCount];
        initPool(poolName);
    }

    private void initPool(@Nonnull String poolName) {
        for (int i = 0; i < instanceCount; i++) {
            sequencers[i] = new SingleThreadExecutionSequencer(String.format("%s-%d", poolName, i));
        }
    }

    @Override
    public ExecutionSequencer getExecutionSequencer() {
        return sequencers[counter.getAndIncrement() % instanceCount];
    }

    @Override
    public void close() throws Exception {
        for(int i = 0; i < instanceCount; i++) {
            if (sequencers[i] != null) sequencers[i].close();
            sequencers[i] = null;
        }
    }
}
