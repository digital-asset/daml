// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps.*
import scalaz.NonEmptyList

private[http] object Collections {
  implicit final class `cdhuc Nel Ops`[A](private val self: NonEmptyList[A]) extends AnyVal {
    def collect[B](f: A PartialFunction B): Option[NonEmptyList[B]] =
      self.list.collect(f).toNel
  }

  def toNonEmptySet[A](as: NonEmptyList[A]): NonEmpty[Set[A]] = {
    import scalaz.syntax.foldable.*
    as.tail.toSet incl1 as.head
  }
}
