// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
import scala.collection.TraversableLike
import scala.collection.generic.CanBuildFrom

package object transaction {

  private[transaction] def sequence[A, B, This, That](seq: TraversableLike[Either[A, B], This])(
      implicit cbf: CanBuildFrom[This, B, That]): Either[A, That] = {
    val b = cbf.apply()
    seq.foreach {
      case Right(a) => b += a
      case Left(e) => return Left(e)
    }
    Right(b.result())
  }

}
