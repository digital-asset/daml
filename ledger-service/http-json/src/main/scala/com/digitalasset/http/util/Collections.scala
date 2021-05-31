// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import scalaz.{NonEmptyList, OneAnd}

object Collections {

  implicit final class `cdhuc Nel Ops`[A](private val self: NonEmptyList[A]) extends AnyVal {
    def collect[B](f: A PartialFunction B): Option[NonEmptyList[B]] =
      self.list.collect(f).toNel
  }

  def toNonEmptySet[A](as: NonEmptyList[A]): OneAnd[Set, A] = {
    import scalaz.syntax.foldable._
    OneAnd(as.head, as.tail.toSet - as.head)
  }
}
