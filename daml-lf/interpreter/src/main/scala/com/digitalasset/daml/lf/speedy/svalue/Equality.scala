// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy
package svalue

import com.daml.lf.value.Value.ContractId
import com.daml.nameof.NameOf

import scala.jdk.CollectionConverters._

private[lf] object Equality {

  // Equality between two SValues of same type.
  // This follows the equality defined in the daml-lf spec.
  @throws[SError.SError]
  def areEqual(x: SValue, y: SValue): Boolean = {
    import SValue._

    var success = true
    var stackX = List(Iterator.single(x))
    var stackY = List(Iterator.single(y))
    // invariant: stackX.length == stackY.length

    @inline
    def push(xs: Iterator[SValue], ys: Iterator[SValue]) = {
      stackX = xs :: stackX
      stackY = ys :: stackY
    }

    @inline
    def pop() = {
      stackX = stackX.tail
      stackY = stackY.tail
    }

    @inline
    def step(tuple: (SValue, SValue)) =
      tuple match {
        case (x: SBuiltinLit, y: SBuiltinLit) =>
          success =
            if (x == y)
              true
            else
              tuple match {
                case (
                      SContractId(cid1 @ ContractId.V1(discriminator1, suffix1)),
                      SContractId(cid2 @ ContractId.V1(discriminator2, suffix2)),
                    ) if discriminator1 == discriminator2 =>
                  if (suffix1.isEmpty)
                    throw SError.SErrorDamlException(
                      interpretation.Error.ContractIdComparability(cid2)
                    )
                  else if (suffix2.isEmpty)
                    throw SError.SErrorDamlException(
                      interpretation.Error.ContractIdComparability(cid1)
                    )
                  else
                    false
                case _ =>
                  false
              }
        case (SEnum(_, _, xRank), SEnum(_, _, yRank)) =>
          success = xRank == yRank
        case (SRecord(_, _, xs), SRecord(_, _, ys)) =>
          push(xs.iterator().asScala, ys.iterator().asScala)
        case (SVariant(_, _, xRank, x), SVariant(_, _, yRank, y)) =>
          push(Iterator.single(x), Iterator.single(y))
          success = xRank == yRank
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
          push(Iterator.single(x), Iterator.single(y))
          success = xType == yType
        case (STypeRep(xType), STypeRep(yType)) =>
          success = xType == yType
        case (_: SPAP, _: SPAP) =>
          throw SError.SErrorDamlException(interpretation.Error.NonComparableValues)
        case (x, y) =>
          throw SError.SErrorCrash(
            NameOf.qualifiedNameOfCurrentFunc,
            s"trying to compare value of different type:\n- $x\n- $y",
          )
      }

    while (success && stackX.nonEmpty) {
      (stackX.head.hasNext, stackY.head.hasNext) match {
        case (true, true) => step((stackX.head.next(), stackY.head.next()))
        case (false, false) => pop()
        case _ => success = false
      }
    }

    success
  }

}
