// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.resources

import com.daml.resources.HasExecutionContext

import scala.concurrent.ExecutionContext

final case class ResourceContext(executionContext: ExecutionContext)

object ResourceContext {

  implicit def executionContext(implicit context: ResourceContext): ExecutionContext =
    context.executionContext

  implicit object `Context has ExecutionContext` extends HasExecutionContext[ResourceContext] {
    override def executionContext(context: ResourceContext): ExecutionContext =
      context.executionContext
  }

}
