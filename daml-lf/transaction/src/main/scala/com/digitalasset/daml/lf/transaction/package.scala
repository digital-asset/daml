// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
import scala.collection.TraversableLike
import scala.collection.generic.CanBuildFrom

package object transaction {

  /** This traversal fails the identity law so is unsuitable for [[scalaz.Traverse]].
    * It is, nevertheless, what is meant sometimes.
    */
  private[transaction] def sequence[A, B, This, That](
      seq: TraversableLike[Either[A, B], This],
  )(implicit cbf: CanBuildFrom[This, B, That]): Either[A, That] =
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
}
