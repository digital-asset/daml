// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.adapter;

public interface ExecutionSequencer extends AutoCloseable {

    /**
     * Runs the argument asynchronously in an environment that assures the following:
     *  - The runnables passed in will be executed one at a time, sequentially.
     *  - The runnables don't need to concern themselves with synchronization and visibility of the
     *    mutable state that they modify if that is accessed only by Runnables that are passed to this method.
     * @param runnable Will be executed asynchronously.
     */
    void sequence(Runnable runnable);
}
