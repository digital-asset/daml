// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter;

/**
 * Provides means to get ExecutionSequencer instances. The objects returned are not necessarily
 * dedicated, there might be other Runnables scheduled on them, but it's guaranteed that these won't
 * interfere and cause thread safety issues.
 */
public interface ExecutionSequencerFactory extends AutoCloseable {
  ExecutionSequencer getExecutionSequencer();
}
