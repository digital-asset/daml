// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.speedy.SError.SErrorCrash
import com.digitalasset.daml.lf.speedy.SValue._
import data.{FrontStack, Ref}

abstract class CostModel(maximumCost: CostModel.Cost) {

  import CostModel._

  val AddNumeric: CostFunction2[Numeric, Numeric]
  val SubNumeric: CostFunction2[Numeric, Numeric]
  val MulNumeric: CostFunction3[Numeric, Numeric, Numeric]
  val DivNumeric: CostFunction3[Numeric, Numeric, Numeric]
  val BRoundNumeric: CostFunction2[Int, Numeric]
  val BCastNumeric: CostFunction3[Numeric, Numeric, Numeric]
  val BShiftNumeric: CostFunction2[Int, Numeric]
  val BAddInt64: CostFunction2[Int64, Int64]
  val BSubInt64: CostFunction2[Int64, Int64]
  val BMulInt64: CostFunction2[Int64, Int64]
  val BDivInt64: CostFunction2[Int64, Int64]
  val BModInt64: CostFunction2[Int64, Int64]
  val BExpInt64: CostFunction2[Int64, Int64]
  val BInt64ToNumeric: CostFunction2[Numeric, Int64]
  val BNumericToInt64: CostFunction1[Numeric]
  val BFoldl = NotDefined
  val BFoldr = NotDefined
  lazy val BTextMapEmpty = BGenMapEmpty
  lazy val BTextMapInsert: CostFunction3[Value, Value, TextMap] = BGenMapInsert
  lazy val BTextMapLookup: CostFunction2[Value, TextMap] = BGenMapLookup
  lazy val BTextMapDelete: CostFunction2[Value, TextMap] = BGenMapDelete
  val BTextMapToList: CostFunction1[TextMap]
  lazy val BTextMapSize = BGenMapSize
  val BGenMapEmpty = NotDefined
  val BGenMapInsert: CostFunction3[Value, Value, GenMap]
  val BGenMapLookup: CostFunction2[Value, GenMap]
  val BGenMapDelete: CostFunction2[Value, GenMap]
  val BGenMapToList: CostFunction1[GenMap]
  val BGenMapKeys: CostFunction1[GenMap]
  val BGenMapValues: CostFunction1[GenMap]
  val BGenMapSize: CostFunction1[GenMap]
  val BAppendText: CostFunction2[Text, Text]
  val BError = NotDefined
  val BInt64ToText: CostFunction1[Int64]
  val BNumericToText: CostFunction1[Numeric]
  val BTimestampToText: CostFunction1[Timestamp]
  val BPartyToText: CostFunction1[Party]
  val BContractIdToText: CostFunction1[ContractId]
  val BCodePointsToText: CostFunction1[FrontStack[Long]]
  val BTextToParty: CostFunction1[Text]
  val BTextToInt64: CostFunction1[Text]
  val BTextToNumeric: CostFunction2[Int, Text]
  val BTextToCodePoints: CostFunction1[Text]
  val BSHA256Text: CostFunction1[Text]
  val BKECCAK256Text: CostFunction1[Text]
  val BSECP256K1Bool: CostFunction1[Text]
  val BDecodeHex: CostFunction1[Text]
  val BEncodeHex: CostFunction1[Text]
  val BTextToContractId: CostFunction1[Text]
  val BDateToUnixDays: CostFunction1[Date]
  val BExplodeText: CostFunction1[Text]
  val BImplodeText = CostAware
  val BTimestampToUnixMicroseconds: CostFunction1[Timestamp]
  val BDateToText: CostFunction1[Date]
  val BUnixDaysToDate: CostFunction1[Int64]
  val BUnixMicrosecondsToTimestamp: CostFunction1[Int64]
  val BEqual: CostFunction2[Value, Value]
  val BLess: CostFunction2[Value, Value]
  val BLessEq: CostFunction2[Value, Value]
  val BGreater: CostFunction2[Value, Value]
  val BGreaterEq: CostFunction2[Value, Value]
  val BEqualList = NotDefined
  val BTrace = NotDefined
  val BCoerceContractId: CostFunction1[ContractId]
  val BScaleBigNumeric = NotDefined // Available in 2.dev
  val BPrecisionBigNumeric = NotDefined // Available in 2.dev
  val BAddBigNumeric = NotDefined // Available in 2.dev
  val BSubBigNumeric = NotDefined // Available in 2.dev
  val BMulBigNumeric = NotDefined // Available in 2.dev
  val BDivBigNumeric = NotDefined // Available in 2.dev
  val BShiftRightBigNumeric = NotDefined // Available in 2.dev
  val BBigNumericToNumeric = NotDefined // Available in 2.dev
  val BNumericToBigNumeric = NotDefined // Available in 2.dev
  val BBigNumericToText = NotDefined // Available in 2.dev
  val BAnyExceptionMessage = NotDefined
  val BTypeRepTyConName = NotDefined
  val BFailWithStatus = NotDefined

  private[this] var totalCost: Cost = 0

  private[speedy] final def update(cost: Cost): Unit = {
    if (totalCost + cost <= maximumCost) {
      totalCost += cost
    } else {
      throw SErrorCrash(
        getClass.getCanonicalName,
        s"Current interpretation has used $totalCost, so an operation with cost $cost will exceed maximum budgeted cost of $maximumCost",
      )
    }
  }

  private[speedy] final def undefined(cost: NotDefined.type): Unit = {}

  private[speedy] final def costAware(cost: CostAware.type): Unit = {}
}

object CostModel {

  type Cost = Int

  type Int64 = Long
  type Numeric = data.Numeric
  type Text = String
  type Date = Long
  type Timestamp = Long
  type TextMap = SMap
  type GenMap = SMap
  type List = SList
  type ContractId = value.Value.ContractId
  type Value = SValue
  type Party = Ref.Party

  object NotDefined

  object CostAware

  sealed abstract class CostFunction1[X] {
    def cost(x: X): Cost
  }

  final case class ConstantCost1[X](c: Cost) extends CostFunction1[X] {
    override def cost(x: X): Cost = c
  }

  sealed abstract class CostFunction2[X, Y] {
    def cost(x: X, y: Y): Cost
  }

  final case class ConstantCost2[X, Y](c: Cost) extends CostFunction2[X, Y] {
    override def cost(x: X, y: Y): Cost = c
  }

  sealed abstract class CostFunction3[X, Y, Z] {
    def cost(x: Y, y: Y, z: Z): Cost
  }

  final case class ConstantCost3[X, Y, Z](c: Cost) extends CostFunction3[X, Y, Z] {
    override def cost(x: Y, y: Y, z: Z): Cost = c
  }

  object EmptyModel extends CostModel(0) {
    override val AddNumeric: CostFunction2[Numeric, Numeric] = ConstantCost2(0)
    override val SubNumeric: CostFunction2[Numeric, Numeric] = ConstantCost2(0)
    override val MulNumeric: CostFunction3[Numeric, Numeric, Numeric] = ConstantCost3(0)
    override val DivNumeric: CostFunction3[Numeric, Numeric, Numeric] = ConstantCost3(0)
    override val BRoundNumeric: CostFunction2[Cost, Numeric] = ConstantCost2(0)
    override val BCastNumeric: CostFunction3[Numeric, Numeric, Numeric] = ConstantCost3(0)
    override val BShiftNumeric: CostFunction2[Int, Numeric] = ConstantCost2(0)
    override val BAddInt64: CostFunction2[Int64, Int64] = ConstantCost2(0)
    override val BSubInt64: CostFunction2[Int64, Int64] = ConstantCost2(0)
    override val BMulInt64: CostFunction2[Int64, Int64] = ConstantCost2(0)
    override val BDivInt64: CostFunction2[Int64, Int64] = ConstantCost2(0)
    override val BModInt64: CostFunction2[Int64, Int64] = ConstantCost2(0)
    override val BExpInt64: CostFunction2[Int64, Int64] = ConstantCost2(0)
    override val BInt64ToNumeric: CostFunction2[Numeric, Int64] = ConstantCost2(0)
    override val BNumericToInt64: CostFunction1[Numeric] = ConstantCost1(0)
    override val BTextMapToList: CostFunction1[TextMap] = ConstantCost1(0)
    override val BGenMapInsert: CostFunction3[Value, Value, GenMap] = ConstantCost3(0)
    override val BGenMapLookup: CostFunction2[Value, GenMap] = ConstantCost2(0)
    override val BGenMapDelete: CostFunction2[Value, GenMap] = ConstantCost2(0)
    override val BGenMapToList: CostFunction1[GenMap] = ConstantCost1(0)
    override val BGenMapKeys: CostFunction1[GenMap] = ConstantCost1(0)
    override val BGenMapValues: CostFunction1[GenMap] = ConstantCost1(0)
    override val BGenMapSize: CostFunction1[GenMap] = ConstantCost1(0)
    override val BAppendText: CostFunction2[Text, Text] = ConstantCost2(0)
    override val BInt64ToText: CostFunction1[Int64] = ConstantCost1(0)
    override val BNumericToText: CostFunction1[Numeric] = ConstantCost1(0)
    override val BTimestampToText: CostFunction1[Timestamp] = ConstantCost1(0)
    override val BPartyToText: CostFunction1[Party] = ConstantCost1(0)
    override val BContractIdToText: CostFunction1[ContractId] = ConstantCost1(0)
    override val BCodePointsToText: CostFunction1[FrontStack[Long]] = ConstantCost1(0)
    override val BTextToParty: CostFunction1[Text] = ConstantCost1(0)
    override val BTextToInt64: CostFunction1[Text] = ConstantCost1(0)
    override val BTextToNumeric: CostFunction2[Int, Text] = ConstantCost2(0)
    override val BTextToCodePoints: CostFunction1[Text] = ConstantCost1(0)
    override val BSHA256Text: CostFunction1[Text] = ConstantCost1(0)
    override val BKECCAK256Text: CostFunction1[Text] = ConstantCost1(0)
    override val BSECP256K1Bool: CostFunction1[Text] = ConstantCost1(0)
    override val BDecodeHex: CostFunction1[Text] = ConstantCost1(0)
    override val BEncodeHex: CostFunction1[Text] = ConstantCost1(0)
    override val BTextToContractId: CostFunction1[Text] = ConstantCost1(0)
    override val BDateToUnixDays: CostFunction1[Date] = ConstantCost1(0)
    override val BExplodeText: CostFunction1[Text] = ConstantCost1(0)
    override val BTimestampToUnixMicroseconds: CostFunction1[Timestamp] = ConstantCost1(0)
    override val BDateToText: CostFunction1[Date] = ConstantCost1(0)
    override val BUnixDaysToDate: CostFunction1[Int64] = ConstantCost1(0)
    override val BUnixMicrosecondsToTimestamp: CostFunction1[Int64] = ConstantCost1(0)
    override val BEqual: CostFunction2[Value, Value] = ConstantCost2(0)
    override val BLess: CostFunction2[Value, Value] = ConstantCost2(0)
    override val BLessEq: CostFunction2[Value, Value] = ConstantCost2(0)
    override val BGreater: CostFunction2[Value, Value] = ConstantCost2(0)
    override val BGreaterEq: CostFunction2[Value, Value] = ConstantCost2(0)
    override val BCoerceContractId: CostFunction1[ContractId] = ConstantCost1(0)
  }
}
