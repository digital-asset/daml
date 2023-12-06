// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.logging.NamedLogging
import org.scalatest.Suite

/** Mixin that provides an execution context for tests.
  * The execution context supports blocking operations, provided they are wrapped in [[scala.concurrent.blocking]]
  * or [[scala.concurrent.Await]].
  */
trait HasExecutionContext extends HasExecutorService { this: Suite with NamedLogging =>
  implicit def parallelExecutionContext: ExecutionContextIdlenessExecutorService = executorService
}
