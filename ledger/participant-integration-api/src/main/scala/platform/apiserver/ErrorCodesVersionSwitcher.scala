// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import io.grpc.StatusRuntimeException

import scala.concurrent.Future

final class ErrorCodesVersionSwitcher(enableErrorCodesV2: Boolean) {
  def choose(
      v1: => StatusRuntimeException,
      v2: => StatusRuntimeException,
  ): StatusRuntimeException = {
    if (enableErrorCodesV2) {
      v2
    } else {
      v1
    }
  }

  def chooseAsFailedFuture[T](
      v1: => StatusRuntimeException,
      v2: => StatusRuntimeException,
  ): Future[T] = {
    Future.failed(choose(v1 = v1, v2 = v2))
  }
}
