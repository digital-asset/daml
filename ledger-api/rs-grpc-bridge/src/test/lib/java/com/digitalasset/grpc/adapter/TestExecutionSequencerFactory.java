// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter;

public class TestExecutionSequencerFactory {
  public static ExecutionSequencerFactory instance =
      new SingleThreadExecutionSequencerPool("rs-grpc-bridge-testing");
}
