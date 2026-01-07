// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.grpc.adapter;

import java.util.concurrent.atomic.AtomicLong;

public class CallCounter {

  private static final AtomicLong callCounter = new AtomicLong(0L);

  public static long getNewCallId() {
    return callCounter.incrementAndGet();
  }
}
