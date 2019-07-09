// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

private[digitalasset] object LawlessTraversals {
  implicit final class `Lawless iterable traversal`[A, This](private val seq: IterableLike[A, This])
      extends AnyVal {
    def traverseEitherStrictly[E, B, That](f: A => Either[E, B])(
        implicit cbf: CanBuildFrom[This, B, That]): Either[E, That] = {
      val that = cbf()
      that.sizeHint(seq)
      val i = seq.iterator
      @tailrec def lp(): Either[B, That] =
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
