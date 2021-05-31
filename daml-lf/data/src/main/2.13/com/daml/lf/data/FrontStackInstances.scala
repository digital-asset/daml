// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import ScalazEqual.{equalBy, toIterableForScalazInstances}

import scalaz.Equal
import scala.collection.mutable.Builder

abstract class FrontStackInstances extends scala.collection.IterableFactory[FrontStack] {
  override def from[A](it: IterableOnce[A]) = (newBuilder ++= it).result()
  override def newBuilder[A]: Builder[A, FrontStack[A]] =
    ImmArray.newBuilder.mapResult(FrontStack(_))
  implicit def equalInstance[A: Equal]: Equal[FrontStack[A]] = {
    import scalaz.std.iterable._
    equalBy(fs => toIterableForScalazInstances(fs.iterator), true)
  }
}
