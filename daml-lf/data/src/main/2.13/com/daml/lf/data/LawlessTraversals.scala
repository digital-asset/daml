// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import scala.annotation.tailrec
import scala.collection.BuildFrom

private[daml] object LawlessTraversals {
  implicit final class `Lawless iterable traversal`[A, This[X] <: Iterable[X]](
      private val seq: This[A])
      extends AnyVal {
    def traverseEitherStrictly[E, B, That](f: A => Either[E, B])(
        implicit cbf: BuildFrom[This[A], B, That]): Either[E, That] = {
      val that = cbf.newBuilder(seq)
      that.sizeHint(seq)
      val i = seq.iterator
      @tailrec def lp(): Either[E, That] =
        if (i.hasNext) f(i.next()) match {
          case Left(b) => Left(b)
          case Right(c) =>
            that += c
            lp()
        } else Right(that.result())
      lp()
    }
  }
}
