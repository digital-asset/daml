// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import ScalazEqual.{equalBy, toIterableForScalazInstances}

import scalaz.Equal

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable

private[data] abstract class FrontStackInstances {

  def apply[T](elements: T*): FrontStack[T] = elements.to[FrontStack]

  implicit def equalInstance[A: Equal]: Equal[FrontStack[A]] = {
    import scalaz.std.iterable._
    equalBy(fs => toIterableForScalazInstances(fs.iterator), true)
  }
  implicit def `FrontStack canBuildFrom`[A]: CanBuildFrom[FrontStack[_], A, FrontStack[A]] =
    new FrontStackInstances.FSCanBuildFrom

  import scala.language.implicitConversions

  /** Enables 2.13-style `to` calls. */
  implicit def `FS companion to CBF`[A](
      self: FrontStack.type
  ): CanBuildFrom[FrontStack[_], A, FrontStack[A]] =
    self.`FrontStack canBuildFrom`
}

private[data] object FrontStackInstances {
  final class FSCanBuildFrom[A] extends CanBuildFrom[FrontStack[_], A, FrontStack[A]] {
    override def apply(from: FrontStack[_]): mutable.Builder[A, FrontStack[A]] = apply()

    override def apply(): mutable.Builder[A, FrontStack[A]] =
      ImmArray.newBuilder[A].mapResult(FrontStack.from)
  }
}
