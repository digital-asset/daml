// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy.svalue

import com.digitalasset.daml.lf.data.FrontStack
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.speedy.SValue._

import scala.annotation.tailrec
import scala.collection.JavaConverters._

private[lf] object Equality {

  // Equality between two SValues of same type.
  // Note it is not reflexive, in other words there is some value `v`
  // such that `areEqual(v, v)` returns `False`).
  // This follows the equality defined in the daml-lf spec.
  def areEqual(x: SValue, y: SValue): Boolean = equality(FrontStack((x, y)))

  private[this] def zipAndPush[X, Y](
      h1: Iterator[X],
      h2: Iterator[Y],
      stack: FrontStack[(X, Y)],
  ): FrontStack[(X, Y)] =
    (stack /: (h1 zip h2))(_.+:(_))

  @tailrec
  private[this] def equality(stack0: FrontStack[(SValue, SValue)]): Boolean = {
    stack0.pop match {
      case Some((tuple, stack)) =>
        tuple match {
          case (x: SPrimLit, y: SPrimLit) =>
            x == y && equality(stack)
          case (SEnum(tyCon1, con1), SEnum(tyCon2, con2)) =>
            tyCon1 == tyCon2 && con1 == con2 && equality(stack)
          case (SRecord(tyCon1, fields1, args1), SRecord(tyCon2, fields2, args2)) =>
            tyCon1 == tyCon2 && (fields1 sameElements fields2) &&
              equality(zipAndPush(args1.iterator().asScala, args2.iterator().asScala, stack))
          case (SVariant(tyCon1, con1, arg1), SVariant(tyCon2, con2, arg2)) =>
            tyCon1 == tyCon2 && con1 == con2 && equality((arg1, arg2) +: stack)
          case (SList(lst1), SList(lst2)) =>
            lst1.length == lst2.length &&
              equality(zipAndPush(lst1.iterator, lst2.iterator, stack))
          case (SOptional(None), SOptional(None)) =>
            equality(stack)
          case (SOptional(Some(v1)), SOptional(Some(v2))) =>
            equality((v1, v2) +: stack)
          case (STextMap(map1), STextMap(map2)) =>
            map1.keySet == map2.keySet && {
              val keys = map1.keys
              equality(zipAndPush(keys.iterator.map(map1), keys.iterator.map(map2), stack))
            }
          case (SGenMap(map1), SGenMap(map2)) =>
            map1.keySet == map2.keySet && {
              val keys = map1.keys
              equality(zipAndPush(keys.iterator.map(map1), keys.iterator.map(map2), stack))
            }
          case (SStruct(fields1, args1), SStruct(fields2, args2)) =>
            (fields1 sameElements fields2) && equality(
              zipAndPush(args1.iterator().asScala, args2.iterator().asScala, stack),
            )
          case (SAny(t1, v1), SAny(t2, v2)) =>
            t1 == t2 && equality((v1, v2) +: stack)
          case (STypeRep(t1), STypeRep(t2)) =>
            t1 == t2 && equality(stack)
          case _ =>
            false
        }
      case _ =>
        true
    }
  }

}
