// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import slick.jdbc.{PositionedParameters, SetParameter}

/** This trait serves as a way to declare how a type `From` can be transformed into a db primitive
  * type `To`. It also serves as an instance of `SetParameter[From]`.
  */
trait ToDbPrimitive[-From, To] extends SetParameter[From] {
  def toDbPrimitive(from: From): To
}

object ToDbPrimitive {
  def apply[From, To](implicit T: ToDbPrimitive[From, To]): ToDbPrimitive[From, To] = T

  def apply[From, To](f: From => To)(implicit
      setParameterTo: SetParameter[To]
  ): ToDbPrimitive[From, To] = new ToDbPrimitive[From, To] {
    override def toDbPrimitive(from: From): To = f(from)

    override def apply(v: From, pp: PositionedParameters): Unit =
      setParameterTo.apply(toDbPrimitive(v), pp)
  }
}
