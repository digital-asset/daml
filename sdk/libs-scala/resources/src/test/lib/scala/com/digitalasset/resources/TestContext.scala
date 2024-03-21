// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import scala.concurrent.ExecutionContext

final class TestContext(val executionContext: ExecutionContext)

object TestContext {
  implicit def `TestContext executionContext`(implicit context: TestContext): ExecutionContext =
    HasExecutionContext.executionContext

  implicit val `TestContext has ExecutionContext`: HasExecutionContext[TestContext] =
    (context: TestContext) => context.executionContext
}
