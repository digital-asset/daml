// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.executors.executors

import com.daml.executors.QueueAwareExecutorService

import java.util.concurrent.ExecutorService
import scala.concurrent.ExecutionContextExecutorService

class QueueAwareExecutionContextExecutorService(
    delegate: ExecutorService,
    name: String,
    reporter: Throwable => Unit,
) extends QueueAwareExecutorService(delegate, name)
    with ExecutionContextExecutorService {
  override def reportFailure(cause: Throwable): Unit = reporter(cause)
}
