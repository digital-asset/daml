// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import scala.collection.IterableLike
import scala.collection.generic.CanBuildFrom
import scala.annotation.tailrec

private[daml] object LawlessTraversals {
  implicit final class `Lawless iterable traversal`[A, This](private val seq: IterableLike[A, This])
      extends AnyVal {
    def traverseEitherStrictly[E, B, That](f: A => Either[E, B])(
        implicit cbf: CanBuildFrom[This, B, That]): Either[E, That] = {
      val that = cbf(seq.repr)
      that.sizeHint(seq)
      val i = seq.iterator
      @tailrec def lp(): Either[E, That] =
        if (i.hasNext) f(i.next) match {
          case Left(b) => Left(b)
          case Right(c) =>
            that += c
            lp()
        } else Right(that.result)
      lp()
    }
  }
}
