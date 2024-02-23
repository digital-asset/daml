// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.logging.NamedLogging
import org.scalatest.Suite

/** Mixin that provides a default execution context for tests.
  * The execution context supports blocking operations, provided they are wrapped in [[scala.concurrent.blocking]]
  * or [[scala.concurrent.Await]].
  */
trait HasExecutionContext extends HasExecutorService { this: Suite & NamedLogging =>
  implicit def parallelExecutionContext: ExecutionContextIdlenessExecutorService = executorService
}
