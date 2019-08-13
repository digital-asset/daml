// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.adapter;

import java.util.concurrent.atomic.AtomicLong;

public class CallCounter {

    private static final AtomicLong callCounter = new AtomicLong(0L);

    public static long getNewCallId() {
        return callCounter.incrementAndGet();
    }
}
