// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.ledger.rxjava.grpc;

import io.grpc.*;
import java.util.concurrent.TimeUnit;

public final class TimeoutInterceptor implements ClientInterceptor {

  private final long duration;
  private final TimeUnit unit;

  public TimeoutInterceptor(long duration, TimeUnit unit) {
    this.duration = duration;
    this.unit = unit;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    return next.newCall(method, callOptions.withDeadlineAfter(duration, unit));
  }
}
