// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import scala.collection.{Factory, mutable}

private[resources] final class UnitCanBuildFrom[T, C[_]] extends Factory[T, Unit] {
  override def fromSpecific(it: IterableOnce[T]) = ()
  override def newBuilder: mutable.Builder[T, Unit] = UnitBuilder
}

private[resources] object UnitBuilder extends mutable.Builder[Any, Unit] {
  override def addOne(elem: Any): this.type = this

  override def clear(): Unit = ()

  override def result(): Unit = ()
}
