// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy.iterable

import com.daml.lf.speedy.SValue
import scala.jdk.CollectionConverters._

// Iterates only over immediate children similar to Haskell’s
// uniplate.
private[speedy] object SValueIterable {
  that =>
  private[iterable] def iterator(v: SValue): Iterator[SValue] = v match {
    case SValue.SPAP(prim, actuals, _) => iterator(prim) ++ actuals.asScala.iterator
    case SValue.SRecord(_, _, values) => values.asScala.iterator
    case SValue.SStruct(_, values) => values.asScala.iterator
    case SValue.SVariant(_, _, _, value) => Iterator(value)
    case SValue.SEnum(_, _, _) => Iterator.empty
    case SValue.SOptional(value) => value.iterator
    case SValue.SList(list) => list.iterator
    case SValue.SMap(_, entries) => entries.iterator.flatMap({ case (k, v) => Iterator(k, v) })
    case SValue.SAny(_, value) => Iterator(value)
    case SValue.STNat(_) => Iterator.empty
    case SValue.STypeRep(_) => Iterator.empty
    case SValue.SToken => Iterator.empty
    case _: SValue.SPrimLit => Iterator.empty
  }

  private def iterator(p: SValue.Prim): Iterator[SValue] = p match {
    case SValue.PBuiltin(_) => Iterator.empty
    case SValue.PClosure(_, _, frame) => frame.iterator.flatMap(that.iterator(_))
  }

  def apply(v: SValue): Iterable[SValue] =
    new Iterable[SValue] {
      override def iterator = that.iterator(v)
    }
}
