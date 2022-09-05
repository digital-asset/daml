// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import scala.concurrent.ExecutionContext

trait HasExecutionContext[Context] {
  def executionContext(context: Context): ExecutionContext
}

object HasExecutionContext {
  def apply[Context](implicit self: HasExecutionContext[Context]): HasExecutionContext[Context] =
    Option(self).get

  implicit def executionContext[Context: HasExecutionContext](implicit
      context: Context
  ): ExecutionContext =
    apply[Context].executionContext(context)

  implicit object `ExecutionContext has itself` extends HasExecutionContext[ExecutionContext] {
    override def executionContext(context: ExecutionContext): ExecutionContext = context
  }

}
