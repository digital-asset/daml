// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable

private[resources] final class UnitCanBuildFrom[T, C[_]] extends CanBuildFrom[C[T], T, Unit] {
  override def apply(): mutable.Builder[T, Unit] = UnitBuilder
  override def apply(from: C[T]): mutable.Builder[T, Unit] = apply()
}

private[resources] object UnitBuilder extends mutable.Builder[Any, Unit] {
  override def +=(elem: Any): this.type = this

  override def clear(): Unit = ()

  override def result(): Unit = ()
}
