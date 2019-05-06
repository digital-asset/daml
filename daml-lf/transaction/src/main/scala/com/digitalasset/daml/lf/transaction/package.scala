// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
import scala.annotation.tailrec
import scala.collection.{IterableLike, TraversableLike}
import scala.collection.generic.CanBuildFrom

package object transaction {

  /** This traversal fails the identity law so is unsuitable for [[scalaz.Traverse]].
    * It is, nevertheless, what is meant sometimes.
    */
  private[transaction] def sequence[A, B, This, That](seq: TraversableLike[Either[A, B], This])(
      implicit cbf: CanBuildFrom[This, B, That]): Either[A, That] =
    seq collectFirst {
      case Left(e) => Left(e)
    } getOrElse {
      val b = cbf.apply()
      seq.foreach {
        case Right(a) => b += a
        case e @ Left(_) => sys.error(s"impossible $e")
      }
      Right(b.result())
    }

  /** This traversal fails the identity law so is unsuitable for [[scalaz.Traverse]].
    * It is, nevertheless, what is meant sometimes.
    */
  private[digitalasset] def traverseEitherStrictly[A, B, C, This, That](seq: IterableLike[A, This])(
      f: A => Either[B, C])(implicit cbf: CanBuildFrom[This, C, That]): Either[B, That] = {
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
