// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.speedy.SValue._
import value.Value
import data.Ref
import sttp.tapir.SchemaType.SOption

abstract class CostModel {

  import CostModel._

  val AddNumeric: Function2[Numeric, Numeric]
  val SubNumeric: Function2[Numeric, Numeric]
  val MulNumeric: Function3[Numeric, Numeric, Numeric]
  val DivNumeric: Function3[Numeric, Numeric, Numeric]
  val BRoundNumeric: Function2[Int, Numeric]
  val BCastNumeric: Function3[Numeric, Numeric, Numeric]
  val BShiftNumeric: Function3[Numeric, Numeric, Numeric]
  val BAddInt64: Function2[Int64, Int64]
  val BSubInt64: Function2[Int64, Int64]
  val BMulInt64: Function2[Int64, Int64]
  val BDivInt64: Function2[Int64, Int64]
  val BModInt64: Function2[Int64, Int64]
  val BExpInt64: Function2[Int64, Int64]
  val BInt64ToNumeric: Function2[Numeric, Int64]
  val BNumericToInt64: Function1[Numeric]
  val BFoldl = NotDefined
  val BFoldr = NotDefined
  lazy val BTextMapEmpty = BGenMapEmpty
  lazy val BTextMapInsert: Function3[Value, Value, TextMap] = BGenMapInsert
  lazy val BTextMapLookup: Function2[Value, TextMap] = BGenMapLookup
  lazy val BTextMapDelete: Function2[Value, TextMap] = BGenMapDelete
  val BTextMapToList: Function1[TextMap]
  lazy val BTextMapSize = BGenMapSize
  val BGenMapEmpty = NotDefined
  val BGenMapInsert: Function3[Value, Value, GenMap]
  val BGenMapLookup: Function2[Value, GenMap]
  val BGenMapDelete: Function2[Value, GenMap]
  val BGenMapKeys: Function1[GenMap]
  val BGenMapValues: Function1[GenMap]
  val BGenMapSize: Function1[GenMap]
  val BAppendText: Function2[Text, Text]
  val BError = NotDefined
  val BInt64ToText: Function1[Int64]
  val BNumericToText: Function1[Numeric]
  val BTimestampToText: Function1[Timestamp]
  val BPartyToText: Function1[Party]
  val BContractIdToText: Function1[ContractId]
  val BCodePointsToText: Function1[List]
  val BTextToParty: Function1[Text]
  val BTextToInt64: Function1[Text]
  val BTextToNumeric: Function1[Text]
  val BTextToCodePoints: Function1[Text]
  val BSHA256Text: Function1[Text]
  val BKECCAK256Text: Function1[Text]
  val BSECP256K1Bool: Function1[Text]
  val BDecodeHex: Function1[Text]
  val BEncodeHex: Function1[Text]
  val BTextToContractId: Function1[Text]
  val BDateToUnixDays: Function1[Date]
  val BExplodeText: Function1[Text]
  val BImplodeText = CostAware
  val BTimestampToUnixMicroseconds: Function1[Timestamp]
  val BDateToText: Function1[Date]
  val BUnixDaysToDate: Function1[Int64]
  val BUnixMicrosecondsToTimestamp: Function1[Int64]
  val BEqual: Function2[Value, Value]
  val BLess: Function2[Value, Value]
  val BLessEq: Function2[Value, Value]
  val BGreater: Function2[Value, Value]
  val BGreaterEq: Function2[Value, Value]
  val BEqualList = NotDefined
  val BTrace = NotDefined
  val BCoerceContractId: Function1[ContractId]
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

}

object CostModel {

  type Cost = Int

  type Int64 = Long
  type Numeric = data.Numeric
  type Text = String
  type Date = Int
  type Timestamp = Long
  type TextMap = SMap
  type GenMap = SMap
  type List = SList
  type ContractId = value.Value.ContractId
  type Value = _
  type Party = Ref.Party

  object NotDefined

  object CostAware

  sealed abstract class Function1[X] {
    def cost(x: X): Cost
  }

  case class Constant1[X](c: Cost) extends Function1[X] {
    override def cost(x: X): Cost = c
  }

  sealed abstract class Function2[X, Y] {
    def cost(x: X, y: Y): Cost
  }

  case class Constant2[X, Y](c: Cost) extends Function2[X, Y] {
    override def cost(x: X, y: Y): Cost = c
  }

  sealed abstract class Function3[X, Y, Z] {
    def cost(x: Y, y: Y, z: Z): Cost
  }

  case class Constant3[X, Y, Z](c: Cost) extends Function3[X, Y, Z] {
    override def cost(x: Y, y: Y, z: Z): Cost = c
  }

  object Empty extends CostModel {
    override val AddNumeric: Function2[Numeric, Numeric] = Constant2(0)
    override val SubNumeric: Function2[Numeric, Numeric] = Constant2(0)
    override val MulNumeric: Function3[Numeric, Numeric, Numeric] = Constant3(0)
    override val DivNumeric: Function3[Numeric, Numeric, Numeric] = Constant3(0)
    override val BRoundNumeric: Function2[Cost, Numeric] = Constant2(0)
    override val BCastNumeric: Function3[Numeric, Numeric, Numeric] = Constant3(0)
    override val BShiftNumeric: Function3[Numeric, Numeric, Numeric] = Constant3(0)
    override val BAddInt64: Function2[Int64, Int64] = Constant2(0)
    override val BSubInt64: Function2[Int64, Int64] = Constant2(0)
    override val BMulInt64: Function2[Int64, Int64] = Constant2(0)
    override val BDivInt64: Function2[Int64, Int64] = Constant2(0)
    override val BModInt64: Function2[Int64, Int64] = Constant2(0)
    override val BExpInt64: Function2[Int64, Int64] = Constant2(0)
    override val BInt64ToNumeric: Function2[Numeric, Int64] = Constant2(0)
    override val BNumericToInt64: Function1[Numeric] = Constant1(0)
    override val BTextMapToList: Function1[TextMap] = Constant1(0)
    override val BGenMapInsert: Function3[Any, Any, GenMap] = Constant3(0)
    override val BGenMapLookup: Function2[Any, GenMap] = Constant2(0)
    override val BGenMapDelete: Function2[Any, GenMap] = Constant2(0)
    override val BGenMapKeys: Function1[GenMap] = Constant1(0)
    override val BGenMapValues: Function1[GenMap] = Constant1(0)
    override val BGenMapSize: Function1[GenMap] = Constant1(0)
    override val BAppendText: Function2[Text, Text] = Constant2(0)
    override val BInt64ToText: Function1[Int64] = Constant1(0)
    override val BNumericToText: Function1[Numeric] = Constant1(0)
    override val BTimestampToText: Function1[Timestamp] = Constant1(0)
    override val BPartyToText: Function1[Party] = Constant1(0)
    override val BContractIdToText: Function1[ContractId] = Constant1(0)
    override val BCodePointsToText: Function1[List] = Constant1(0)
    override val BTextToParty: Function1[Text] = Constant1(0)
    override val BTextToInt64: Function1[Text] = Constant1(0)
    override val BTextToNumeric: Function1[Text] = Constant1(0)
    override val BTextToCodePoints: Function1[Text] = Constant1(0)
    override val BSHA256Text: Function1[Text] = Constant1(0)
    override val BKECCAK256Text: Function1[Text] = Constant1(0)
    override val BSECP256K1Bool: Function1[Text] = Constant1(0)
    override val BDecodeHex: Function1[Text] = Constant1(0)
    override val BEncodeHex: Function1[Text] = Constant1(0)
    override val BTextToContractId: Function1[Text] = Constant1(0)
    override val BDateToUnixDays: Function1[Date] = Constant1(0)
    override val BExplodeText: Function1[Text] = Constant1(0)
    override val BTimestampToUnixMicroseconds: Function1[Timestamp] = Constant1(0)
    override val BDateToText: Function1[Date] = Constant1(0)
    override val BUnixDaysToDate: Function1[Int64] = Constant1(0)
    override val BUnixMicrosecondsToTimestamp: Function1[Int64] = Constant1(0)
    override val BEqual: Function2[Any, Any] = Constant2(0)
    override val BLess: Function2[Any, Any] = Constant2(0)
    override val BLessEq: Function2[Any, Any] = Constant2(0)
    override val BGreater: Function2[Any, Any] = Constant2(0)
    override val BGreaterEq: Function2[Any, Any] = Constant2(0)
    override val BCoerceContractId: Function1[ContractId] = Constant1(0)
  }

}
