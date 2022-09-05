// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter;

public interface ExecutionSequencer extends AutoCloseable {

  /**
   * Runs the argument asynchronously in an environment that assures the following: - The runnables
   * passed in will be executed one at a time, sequentially. - The runnables don't need to concern
   * themselves with synchronization and visibility of the mutable state that they modify if that is
   * accessed only by Runnables that are passed to this method.
   *
   * @param runnable Will be executed asynchronously.
   */
  void sequence(Runnable runnable);
}
