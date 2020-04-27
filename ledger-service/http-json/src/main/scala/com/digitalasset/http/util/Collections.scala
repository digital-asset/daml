// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import scalaz.{NonEmptyList, OneAnd, \/}

import scala.collection.TraversableLike

object Collections {

  implicit final class `cdhuc TraversableOps`[A, Self](private val self: TraversableLike[A, Self])
      extends AnyVal {

    import collection.generic.CanBuildFrom

    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    def partitionMap[E, B, Es, That](f: A => E \/ B)(
        implicit es: CanBuildFrom[Self, E, Es],
        that: CanBuildFrom[Self, B, That]): (Es, That) = {
      val esb = es(self.repr)
      val thatb = that(self.repr)
      self foreach { a =>
        f(a) fold (esb.+=, thatb.+=)
      }
      (esb.result, thatb.result)
    }
  }

  implicit final class `cdhuc Nel Ops`[A](private val self: NonEmptyList[A]) extends AnyVal {
    def collect[B](f: A PartialFunction B): Option[NonEmptyList[B]] =
      self.list.collect(f).toNel
  }

  def toNonEmptySet[A](as: NonEmptyList[A]): OneAnd[Set, A] = {
    import scalaz.syntax.foldable._
    OneAnd(as.head, as.tail.toSet - as.head)
  }
}
