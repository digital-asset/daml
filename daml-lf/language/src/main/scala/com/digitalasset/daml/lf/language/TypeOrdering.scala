// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package language

import Ast._
import com.daml.nameof.NameOf
import data.Ref

object TypeOrdering extends Ordering[Type] {

  @throws[IllegalArgumentException]
  def compare(x: Type, y: Type): Int = {

    var diff = 0
    var stackX = List(Iterator.single(x))
    var stackY = List(Iterator.single(y))
    // invariant: stackX.length == stackY.length

    @inline
    def push(xs: Iterator[Type], ys: Iterator[Type]): Unit = {
      stackX = xs :: stackX
      stackY = ys :: stackY
    }

    @inline
    def pop(): Unit = {
      stackX = stackX.tail
      stackY = stackY.tail
    }

    @inline
    def compareNamesLexicographically(xs: Iterator[Ref.Name], ys: Iterator[Ref.Name]): Unit = {
      while (diff == 0 && xs.hasNext && ys.hasNext) diff = xs.next() compare ys.next()
      if (diff == 0) diff = xs.hasNext compare ys.hasNext
    }

    @inline
    def step(tuple: (Type, Type)): Unit =
      tuple match {
        case (Ast.TBuiltin(b1), Ast.TBuiltin(b2)) =>
          diff = builtinTypeIdx(b1) compare builtinTypeIdx(b2)
        case (Ast.TTyCon(con1), Ast.TTyCon(con2)) =>
          diff = con1 compare con2
        case (Ast.TNat(n1), Ast.TNat(n2)) =>
          diff = n1 compareTo n2
        case (Ast.TStruct(xs), Ast.TStruct(ys)) =>
          compareNamesLexicographically(xs.names, ys.names)
          push(xs.values, ys.values)
        case (Ast.TApp(x1, x2), Ast.TApp(y1, y2)) =>
          push(Iterator(x1, x2), Iterator(y1, y2))
        case (t1, t2) =>
          diff = typeRank(t1) compareTo typeRank(t2)
      }

    while (diff == 0 && stackX.nonEmpty) {
      diff = stackX.head.hasNext compare stackY.head.hasNext
      if (diff == 0)
        if (stackX.head.hasNext)
          step((stackX.head.next(), stackY.head.next()))
        else
          pop()
    }

    diff
  }

  private[this] val builtinTypeIdx =
    // must be in the same order as declared in the archive protobuf
    // no need to be overly careful though since we have a test enforcing this
    List(
      Ast.BTUnit,
      Ast.BTBool,
      Ast.BTInt64,
      Ast.BTText,
      Ast.BTTimestamp,
      Ast.BTParty,
      Ast.BTList,
      Ast.BTUpdate,
      Ast.BTScenario,
      Ast.BTDate,
      Ast.BTContractId,
      Ast.BTOptional,
      Ast.BTArrow,
      Ast.BTTextMap,
      Ast.BTNumeric,
      Ast.BTAny,
      Ast.BTTypeRep,
      Ast.BTGenMap,
      Ast.BTBigNumeric,
      Ast.BTRoundingMode,
      Ast.BTAnyException,
    ).zipWithIndex.toMap

  private[this] def typeRank(typ: Ast.Type): Int =
    typ match {
      case Ast.TBuiltin(_) => 0
      case Ast.TTyCon(_) => 1
      case Ast.TNat(_) => 2
      case Ast.TStruct(_) => 3
      case Ast.TApp(_, _) => 4
      case Ast.TVar(_) | Ast.TForall(_, _) | Ast.TSynApp(_, _) =>
        InternalError.illegalArgumentException(
          NameOf.qualifiedNameOfCurrentFunc,
          s"cannot compare types $typ",
        )
    }

}
