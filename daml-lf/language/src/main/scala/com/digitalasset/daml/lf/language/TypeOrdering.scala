// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

import com.daml.lf.data.{FrontStack, FrontStackCons, ImmArray}

import scala.annotation.tailrec

object TypeOrdering extends Ordering[Ast.Type] {

  private def zipAndPush[X, Y](
      xs: Iterator[X],
      ys: Iterator[Y],
      stack: FrontStack[(X, Y)],
  ): FrontStack[(X, Y)] =
    (xs zip ys).to[ImmArray] ++: stack

  val builtinTypeIdx =
    List(
      Ast.BTUnit,
      Ast.BTBool,
      Ast.BTInt64,
      Ast.BTText,
      Ast.BTNumeric,
      Ast.BTTimestamp,
      Ast.BTDate,
      Ast.BTParty,
      Ast.BTContractId,
      Ast.BTArrow,
      Ast.BTOptional,
      Ast.BTList,
      Ast.BTTextMap,
      Ast.BTGenMap,
      Ast.BTAny,
      Ast.BTTypeRep,
      Ast.BTUpdate,
      Ast.BTScenario
    ).zipWithIndex.toMap

  private def typeRank(typ: Ast.Type): Int =
    typ match {
      case Ast.TBuiltin(_) => 0
      case Ast.TTyCon(_) => 1
      case Ast.TNat(_) => 2
      case Ast.TStruct(_) => 3
      case Ast.TApp(_, _) => 4
      case Ast.TVar(_) | Ast.TForall(_, _) | Ast.TSynApp(_, _) =>
        throw new IllegalArgumentException(s"cannot compare types $typ")
    }

  @tailrec
  // Any two ground types (types without variable nor quantifiers) can be compared.
  private[this] def compareType(x: Int, stack0: FrontStack[(Ast.Type, Ast.Type)]): Int =
    stack0 match {
      case FrontStack() =>
        x
      case FrontStackCons(tuple, stack) =>
        if (x != 0) x
        else
          tuple match {
            case (Ast.TBuiltin(b1), Ast.TBuiltin(b2)) =>
              compareType(builtinTypeIdx(b1) compareTo builtinTypeIdx(b2), stack)
            case (Ast.TTyCon(con1), Ast.TTyCon(con2)) =>
              compareType(con1 compare con2, stack)
            case (Ast.TNat(n1), Ast.TNat(n2)) =>
              compareType(n1 compareTo n2, stack)
            case (Ast.TStruct(fields1), Ast.TStruct(fields2)) =>
              compareType(
                Ordering.Iterable[String].compare(fields1.names.toSeq, fields2.names.toSeq),
                zipAndPush(fields1.iterator.map(_._2), fields2.iterator.map(_._2), stack)
              )
            case (Ast.TApp(t11, t12), Ast.TApp(t21, t22)) =>
              compareType(0, (t11, t21) +: (t12, t22) +: stack)
            case (t1, t2) =>
              // This case only occurs when t1 and t2 have different ranks.
              val x = typeRank(t1) compareTo typeRank(t2)
              assert(x != 0)
              x
          }
    }

  // undefined if `typ1` or `typ2` contains `TVar`, `TForAll`, or `TSynApp`.
  def compare(typ1: Ast.Type, typ2: Ast.Type): Int =
    compareType(0, FrontStack((typ1, typ2)))

}
