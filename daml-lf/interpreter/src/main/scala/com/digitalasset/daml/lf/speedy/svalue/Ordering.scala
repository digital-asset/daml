// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy
package svalue

import data.{Bytes, Utf8}
import language.TypeOrdering
import value.Value.ContractId

import scala.collection.JavaConverters._

object Ordering extends scala.math.Ordering[SValue] {

  @throws[SError.SErrorCrash]
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
        case (SGenMap(_, xMap), SGenMap(_, yMap)) =>
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
          throw SError.SErrorCrash("functions are not comparable")
        // We should never hit this case at runtime.
        case _ =>
          throw SError.SErrorCrash("BUG: comparison of incomparable values")
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
  private[this] def compareCid(cid1: ContractId, cid2: ContractId): Int =
    (cid1, cid2) match {
      case (ContractId.V0(s1), ContractId.V0(s2)) =>
        s1 compareTo s2
      case (ContractId.V0(_), ContractId.V1(_, _)) =>
        -1
      case (ContractId.V1(_, _), ContractId.V0(_)) =>
        +1
      case (ContractId.V1(hash1, suffix1), ContractId.V1(hash2, suffix2)) =>
        val c1 = crypto.Hash.ordering.compare(hash1, hash2)
        if (c1 != 0)
          c1
        else if (suffix1.isEmpty == suffix2.isEmpty)
          Bytes.ordering.compare(suffix1, suffix2)
        else
          throw SError.SErrorCrash(
            "Conflicting discriminators between a local and global contract id")
    }

}
