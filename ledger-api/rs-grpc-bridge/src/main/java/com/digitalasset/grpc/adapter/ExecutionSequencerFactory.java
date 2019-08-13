// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.adapter;

/**
 * Provides means to get ExecutionSequencer instances.
 * The objects returned are not necessarily dedicated, there might be other Runnables scheduled on them,
 * but it's guaranteed that these won't interfere and cause thread safety issues.
 */
public interface ExecutionSequencerFactory extends AutoCloseable {
    ExecutionSequencer getExecutionSequencer();
}
