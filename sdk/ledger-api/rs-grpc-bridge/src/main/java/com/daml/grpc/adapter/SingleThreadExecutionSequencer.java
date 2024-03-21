// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Ensures serial execution and thread safety of Runnables by using a single thread underneath. */
public class SingleThreadExecutionSequencer implements ExecutionSequencer {

  private static final Logger logger =
      LoggerFactory.getLogger(SingleThreadExecutionSequencer.class);

  private final ExecutorService executor;

  SingleThreadExecutionSequencer(String name) {
    executor =
        Executors.newSingleThreadExecutor(
            runnable -> {
              Thread thread = new Thread(runnable);
              thread.setName(name);
              thread.setDaemon(true);
              thread.setUncaughtExceptionHandler(
                  (t, e) ->
                      logger.error("Unhandled exception in SingleThreadExecutionSequencer.", e));
              return thread;
            });
  }

  @Override
  public void sequence(Runnable runnable) {
    executor.execute(runnable);
  }

  @Override
  public void close() throws Exception {
    executor.shutdown();
    executor.awaitTermination(30L, TimeUnit.SECONDS);
  }
}
