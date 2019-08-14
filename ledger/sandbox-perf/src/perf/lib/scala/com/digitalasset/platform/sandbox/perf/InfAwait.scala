// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.perf

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

trait InfAwait {

  protected def await[T](future: Future[T]): T =
    Await.result(future, Duration.Inf) // Timeout should be defined by the test runner.
}
