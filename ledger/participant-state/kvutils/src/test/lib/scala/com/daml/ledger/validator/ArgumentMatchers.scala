// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import org.mockito.{ArgumentMatcher, ArgumentMatchersSugar}

import scala.concurrent.ExecutionContext

trait ArgumentMatchers {

  import ArgumentMatchersSugar._

  def anyExecutionContext: ExecutionContext = any[ExecutionContext]

  def iterableOf[T](size: Int): Iterable[T] =
    argThat[Iterable[T]](new ArgumentMatcher[Iterable[T]] {
      override def matches(argument: Iterable[T]): Boolean = argument.size == size

      override def toString: String = s"iterable of size $size"
    })
}

object ArgumentMatchers extends ArgumentMatchers
