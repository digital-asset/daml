// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps._
import scalaz.{Apply, Foldable1, NonEmptyList, Semigroup}
import scalaz.syntax.foldable1._

private[http] object Collections {
  implicit final class `cdhuc Nel Ops`[A](private val self: NonEmptyList[A]) extends AnyVal {
    def collect[B](f: A PartialFunction B): Option[NonEmptyList[B]] =
      self.list.collect(f).toNel
  }

  implicit final class `cdhuc Foldable1 Ops`[F[_], A](private val self: F[A]) extends AnyVal {

    /** Foldable1 version of [[scalaz.Foldable#foldMapM]] */
    def foldMapM1[G[_]: Apply, B: Semigroup](f: A => G[B])(implicit F: Foldable1[F]): G[B] = {
      implicit val GB: Semigroup[G[B]] = Semigroup.liftSemigroup
      self foldMap1 f
    }
  }

  def toNonEmptySet[A](as: NonEmptyList[A]): NonEmpty[Set[A]] = {
    import scalaz.syntax.foldable._
    as.tail.toSet incl1 as.head
  }
}
