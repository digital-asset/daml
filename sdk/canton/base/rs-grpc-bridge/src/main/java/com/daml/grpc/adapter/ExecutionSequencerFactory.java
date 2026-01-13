// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.grpc.adapter;

/**
 * Provides means to get ExecutionSequencer instances. The objects returned are not necessarily
 * dedicated, there might be other Runnables scheduled on them, but it's guaranteed that these won't
 * interfere and cause thread safety issues.
 */
public interface ExecutionSequencerFactory extends AutoCloseable {
  ExecutionSequencer getExecutionSequencer();
}
