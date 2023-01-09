// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.perf

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

trait InfAwait {

  protected def await[T](future: Future[T]): T =
    Await.result(future, Duration.Inf) // Timeout should be defined by the test runner.
}
