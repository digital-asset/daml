// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.adapter;

public class TestExecutionSequencerFactory {
    public static ExecutionSequencerFactory instance = new SingleThreadExecutionSequencerPool("rs-grpc-bridge-testing");
}
