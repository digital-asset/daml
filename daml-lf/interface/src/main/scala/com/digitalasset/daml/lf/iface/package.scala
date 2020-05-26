// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import java.{util => j}

import com.daml.lf.data.Ref

import scala.collection.generic.CanBuildFrom
import scala.collection.TraversableLike

// Types to be used internally
package object iface {

  @deprecated("Use TextMap", since = "0.13.38")
  val PrimTypeMap = PrimTypeTextMap

  type FieldWithType = (Ref.Name, Type)

  private[iface] def lfprintln(
      @deprecated("shut up unused arguments warning", "") s: => String): Unit = ()

  /** cf. scalaz.MonadPlus#separate */
  private[iface] def partitionEithers[A, B, Coll, AS, BS](abs: TraversableLike[Either[A, B], Coll])(
      implicit AS: CanBuildFrom[Coll, A, AS],
      BS: CanBuildFrom[Coll, B, BS]): (AS, BS) =
    (abs collect { case Left(a) => a }, abs collect { case Right(a) => a })

  private[iface] def toOptional[A](o: Option[A]): j.Optional[A] =
    o.fold(j.Optional.empty[A])(j.Optional.of)
}
