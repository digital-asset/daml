// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy
package svalue

import com.daml.nameof.NameOf
import data.{Bytes, Utf8}
import language.TypeOrdering
import value.Value.ContractId

import scala.jdk.CollectionConverters._

object Ordering extends scala.math.Ordering[SValue] {

  @throws[SError.SError]
  // Ordering between two SValues of same type.
  // This follows the equality defined in the daml-lf spec.
  def compare(x: SValue, y: SValue): Int = {
    import SValue._

    var diff = 0
    var stackX = List(Iterator.single(x))
    var stackY = List(Iterator.single(y))
    // invariant: stackX.length == stackY.length

    @inline
    def push(xs: Iterator[SValue], ys: Iterator[SValue]): Unit = {
      stackX = xs :: stackX
      stackY = ys :: stackY
    }

    @inline
    def pop(): Unit = {
      stackX = stackX.tail
      stackY = stackY.tail
    }

    @inline
    def step(tuple: (SValue, SValue)) =
      tuple match {
        case (SUnit, SUnit) =>
          ()
        case (SBool(x), SBool(y)) =>
          diff = x compareTo y
        case (SInt64(x), SInt64(y)) =>
          diff = x compareTo y
        case (SNumeric(x), SNumeric(y)) =>
          diff = x compareTo y
        case (SBigNumeric(x), SBigNumeric(y)) =>
          diff = x compareTo y
        case (SText(x), SText(y)) =>
          diff = Utf8.Ordering.compare(x, y)
        case (SDate(x), SDate(y)) =>
          diff = x compareTo y
        case (STimestamp(x), STimestamp(y)) =>
          diff = x compareTo y
        case (SParty(x), SParty(y)) =>
          // parties are ASCII, so UTF16 comparison matches UTF8 comparison.
          diff = x compareTo y
        case (SContractId(x), SContractId(y)) =>
          diff = compareCid(x, y)
        case (SEnum(_, _, xRank), SEnum(_, _, yRank)) =>
          diff = xRank compareTo yRank
        case (SRecord(_, _, xs), SRecord(_, _, ys)) =>
          push(xs.iterator().asScala, ys.iterator().asScala)
        case (SVariant(_, _, xRank, x), SVariant(_, _, yRank, y)) =>
          diff = xRank compareTo yRank
          push(Iterator.single(x), Iterator.single(y))
        case (SList(xs), SList(ys)) =>
          push(xs.iterator, ys.iterator)
        case (SOptional(xOpt), SOptional(yOpt)) =>
          push(xOpt.iterator, yOpt.iterator)
        case (SMap(_, xMap), SMap(_, yMap)) =>
          push(
            new InterlacedIterator(xMap.keys.iterator, xMap.values.iterator),
            new InterlacedIterator(yMap.keys.iterator, yMap.values.iterator),
          )
        case (SStruct(_, xs), SStruct(_, ys)) =>
          push(xs.iterator().asScala, ys.iterator().asScala)
        case (SAny(xType, x), SAny(yType, y)) =>
          diff = TypeOrdering.compare(xType, yType)
          push(Iterator.single(x), Iterator.single(y))
        case (STypeRep(xType), STypeRep(yType)) =>
          diff = TypeOrdering.compare(xType, yType)
        case (_: SPAP, _: SPAP) =>
          throw SError.SErrorDamlException(interpretation.Error.NonComparableValues)
        // We should never hit this case at runtime.
        case _ =>
          throw SError.SErrorCrash(
            NameOf.qualifiedNameOfCurrentFunc,
            s"trying to compare value of different type:\n- $x\n- $y",
          )
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
  @inline
  private[this] def compareCid(cid1: ContractId, cid2: ContractId): Int = {
    (cid1, cid2) match {
      case (
            cid1 @ ContractId.V1(discriminator1, suffix1),
            cid2 @ ContractId.V1(discriminator2, suffix2),
          ) =>
        val c1 = crypto.Hash.ordering.compare(discriminator1, discriminator2)
        if (c1 != 0) {
          c1
        } else if (suffix1.isEmpty) {
          if (suffix2.isEmpty) {
            0
          } else {
            throw SError.SErrorDamlException(interpretation.Error.ContractIdComparability(cid2))
          }
        } else {
          if (suffix2.isEmpty) {
            throw SError.SErrorDamlException(interpretation.Error.ContractIdComparability(cid1))
          } else {
            Bytes.ordering.compare(suffix1, suffix2)
          }
        }
    }
  }

}
