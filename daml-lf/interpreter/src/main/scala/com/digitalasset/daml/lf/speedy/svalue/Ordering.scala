// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy
package svalue

import com.digitalasset.daml.lf.data.{FrontStack, FrontStackCons, ImmArray, Ref, Utf8}
import com.digitalasset.daml.lf.data.ScalazEqual._
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object Ordering extends scala.math.Ordering[SValue] {

  private def zipAndPush[X, Y](
      xs: Iterator[X],
      ys: Iterator[Y],
      stack: FrontStack[(X, Y)],
  ): FrontStack[(X, Y)] =
    (xs zip ys).to[ImmArray] ++: stack

  private def compareIdentifier(name1: Ref.TypeConName, name2: Ref.TypeConName): Int = {
    val c1 = name1.packageId compareTo name2.packageId
    lazy val c2 = name1.qualifiedName.module compareTo name2.qualifiedName.module
    def c3 = name1.qualifiedName.name compareTo name2.qualifiedName.name
    if (c1 != 0) c1 else if (c2 != 0) c2 else c3
  }

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
              compareType(compareIdentifier(con1, con2), stack)
            case (Ast.TNat(n1), Ast.TNat(n2)) =>
              compareType(n1 compareTo n2, stack)
            case (Ast.TStruct(fields1), Ast.TStruct(fields2)) =>
              compareType(
                math.Ordering
                  .Iterable[String]
                  .compare(fields1.toSeq.map(_._1), fields2.toSeq.map(_._1)),
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
  private def compareType(typ1: Ast.Type, typ2: Ast.Type): Int =
    compareType(0, FrontStack((typ1, typ2)))

  private def compareText(text1: String, text2: String): Int =
    Utf8.Ordering.compare(text1, text2)

  private def isHexa(s: String): Boolean =
    s.forall(x => '0' < x && x < '9' || 'a' < x && x < 'f')

  @inline
  private val underlyingCidDiscriminatorLength = 65

  private def compareCid(cid1: Ref.ContractIdString, cid2: Ref.ContractIdString): Int = {
    // FIXME https://github.com/digital-asset/daml/issues/2256
    // cleanup this function

    val lim = cid1.length min cid2.length

    @tailrec
    def lp(i: Int): Int =
      if (i < lim) {
        val x = cid1(i)
        val y = cid2(i)
        if (x != y) x compareTo y else lp(i + 1)
      } else {
        if (lim == underlyingCidDiscriminatorLength && (//
          cid1.length != underlyingCidDiscriminatorLength && isHexa(cid2) || //
          cid2.length != underlyingCidDiscriminatorLength && isHexa(cid1)))
          throw new IllegalArgumentException(
            "Conflicting discriminators between a local and global contract id")
        else
          cid1.length compareTo cid2.length
      }

    lp(0)
  }
  @tailrec
  // Only value of the same type can be compared.
  private[this] def compareValue(stack0: FrontStack[(SValue, SValue)]): Int =
    stack0 match {
      case FrontStack() =>
        0
      case FrontStackCons(tuple, stack) =>
        val (x, toPush) = tuple.match2 {
          case SUnit => {
            case SUnit =>
              0 -> ImmArray.empty
          }
          case SBool(b1) => {
            case SBool(b2) =>
              (b1 compareTo b2) -> ImmArray.empty
          }
          case SInt64(i1) => {
            case SInt64(i2) =>
              (i1 compareTo i2) -> ImmArray.empty
          }
          case STimestamp(ts1) => {
            case STimestamp(ts2) =>
              (ts1.micros compareTo ts2.micros) -> ImmArray.empty
          }
          case SDate(d1) => {
            case SDate(d2) =>
              (d1.days compareTo d2.days) -> ImmArray.empty
          }
          case SNumeric(n1) => {
            case SNumeric(n2) =>
              (n1 compareTo n2) -> ImmArray.empty
          }
          case SText(t1) => {
            case SText(t2) =>
              (compareText(t1, t2)) -> ImmArray.empty
          }
          case SParty(p1) => {
            case SParty(p2) =>
              (compareText(p1, p2)) -> ImmArray.empty
          }
          case SContractId(AbsoluteContractId(coid1)) => {
            case SContractId(AbsoluteContractId(coid2)) =>
              compareCid(coid1, coid2) -> ImmArray.empty
          }
          case STypeRep(t1) => {
            case STypeRep(t2) =>
              compareType(t1, t2) -> ImmArray.empty
          }
          case SEnum(_, con1) => {
            case SEnum(_, con2) =>
              // FIXME https://github.com/digital-asset/daml/issues/2256
              // should not compare constructor syntactically
              (con1 compareTo con2) -> ImmArray.empty
          }
          case SRecord(_, _, args1) => {
            case SRecord(_, _, args2) =>
              0 -> (args1.iterator().asScala zip args2.iterator().asScala).to[ImmArray]
          }
          case SVariant(_, con1, arg1) => {
            case SVariant(_, con2, arg2) =>
              // FIXME https://github.com/digital-asset/daml/issues/2256
              // should not compare constructor syntactically
              (con1 compareTo con2) -> ImmArray((arg1, arg2))
          }
          case SList(FrontStack()) => {
            case SList(l2) =>
              (false compareTo l2.nonEmpty) -> ImmArray.empty
          }
          case SList(FrontStackCons(head1, tail1)) => {
            case SList(FrontStackCons(head2, tail2)) =>
              0 -> ImmArray((head1, head2), (SList(tail1), SList(tail2)))
            case SList(FrontStack()) =>
              1 -> ImmArray.empty
          }
          case SOptional(v1) => {
            case SOptional(v2) =>
              (v1.nonEmpty compareTo v2.nonEmpty) -> (v1.iterator zip v2.iterator).to[ImmArray]
          }
          case map1: STextMap => {
            case map2: STextMap =>
              0 -> ImmArray((toList(map1), toList(map2)))
          }
          case SStruct(_, args1) => {
            case SStruct(_, args2) =>
              0 -> (args1.iterator().asScala zip args2.iterator().asScala).to[ImmArray]
          }
          case SAny(t1, v1) => {
            case SAny(t2, v2) =>
              compareType(t1, t2) -> ImmArray((v1, v2))
          }
          case SContractId(_) => {
            case SContractId(_) =>
              throw new IllegalAccessException("Try to compare relative contract ids")
          }
          case SPAP(_, _, _) => {
            case SPAP(_, _, _) =>
              throw new IllegalAccessException("Try to compare functions")
          }
        }(fallback = throw new IllegalAccessException("Try to compare unrelated type"))
        if (x != 0)
          x
        else
          compareValue(toPush ++: stack)
    }

  def compare(v1: SValue, v2: SValue): Int =
    compareValue(FrontStack((v1, v2)))

}
