// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter;

import java.util.concurrent.atomic.AtomicLong;

public class CallCounter {

  private static final AtomicLong callCounter = new AtomicLong(0L);

  public static long getNewCallId() {
    return callCounter.incrementAndGet();
  }
}
