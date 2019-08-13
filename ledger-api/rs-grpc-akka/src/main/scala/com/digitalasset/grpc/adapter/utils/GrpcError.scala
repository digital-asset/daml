// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.adapter.utils

import io.grpc.{Status, StatusException, StatusRuntimeException}

object GrpcError {
  def apply(throwable: Throwable): Throwable =
    if (throwable.isInstanceOf[StatusException] || throwable
        .isInstanceOf[StatusRuntimeException]) throwable
    else Status.fromThrowable(throwable).asException
}
