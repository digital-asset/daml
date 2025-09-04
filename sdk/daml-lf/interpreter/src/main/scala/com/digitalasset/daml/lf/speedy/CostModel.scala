// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.speedy.SValue._
import data.{FrontStack, Ref}

abstract class CostModel {

  import CostModel._

  /* Builtins */
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

  /* Expr */

  val EVar: CostConstant = CostConstant.Null
  val EVal: CostConstant = CostConstant.Null
  val EBuiltinFun: CostConstant = CostConstant.Null
  val EBuiltinLit: CostConstant = CostConstant.Null
  val ERecCon: CostFunction1[Int] = CostFunction1.Null
  val ERecProj: CostFunction1[Int] = CostFunction1.Null
  val ERecUp: CostFunction1[Int] = CostFunction1.Null
  val EVariantCon: CostConstant = CostConstant.Null
  val EEnumCon: CostConstant = CostConstant.Null
  val EStructCon: CostFunction1[Int] = CostFunction1.Null
  val EStructProj: CostFunction1[Int] = CostFunction1.Null
  val EStructUp: CostFunction1[Int] = CostFunction1.Null
  val EApp: CostConstant = CostConstant.Null
  val EAbs: CostConstant = CostConstant.Null
  val ECase: NotDefined = NotDefined
  val ELet: CostConstant = CostConstant.Null
  val ENil: CostConstant = CostConstant.Null
  val ECons: CostConstant = CostConstant.Null
  val ENone: CostConstant = CostConstant.Null
  val ESome: CostConstant = CostConstant.Null
  val EToAny: CostConstant = CostConstant.Null
  val EFromAny: CostConstant = CostConstant.Null
  val ETypeRep: CostConstant = CostConstant.Null
  val EToInterface: CostConstant = CostConstant.Null
  val EFromInterface: CostConstant = CostConstant.Null
  val EUnsafeFromInterface: CostConstant = CostConstant.Null
  val EToRequiredInterface: CostConstant = CostConstant.Null
  val EFromRequiredInterface: CostConstant = CostConstant.Null
  val EUnsafeFromRequiredInterface: CostConstant = CostConstant.Null
  val ECallInterface: CostConstant = EVal
  val EInterfaceTemplateTypeRep: CostConstant = CostConstant.Null
  val ESignatoryInterface = EVal
  val EObserverInterface = EVal
  val EViewInterface = EVal

  /* Kontinuation */

  val KPure: NotDefined = NotDefined
  val KOverApp: NotDefined = NotDefined
  val KPushTo: NotDefined = NotDefined
  val KFoldl: NotDefined = NotDefined
  val KFoldr: NotDefined = NotDefined
  val KCacheVal: NotDefined = NotDefined
  val KCloseExercise: NotDefined = NotDefined
  val KCheckChoiceGuard: NotDefined = NotDefined // use in 2.dev
  val KLabelClosure: NotDefined = NotDefined // only use when profiling is on
  val KLeaveClosure: NotDefined = NotDefined // only use when profiling is on

  val KTryCatchHandler: NotDefined = NotDefined
  val KPreventException: NotDefined = NotDefined // does nothing when executed
  val KConvertingException: NotDefined = NotDefined // does nothing when executed

  // cost to add n cells to the continuation stack
  val KontStackIncrease: CostFunction1[Int] = CostFunction1.Null
  // cost to add n cells to the env
  val EnvIncrease: CostFunction1[Int] = CostFunction1.Null

}

object CostModel {

  type Cost = Long

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

  object Cost {
    val MaxValue: Cost = Long.MaxValue
  }

  val NotDefined = CostConstant.Null
  type NotDefined = NotDefined.type

  val CostAware = CostConstant.Null
  type CostAware = CostAware.type

  final case class CostConstant(cost: Cost)

  object CostConstant {
    val Null = CostConstant(0)
  }

  abstract class CostFunction1[-X] {
    def cost(x: X): Cost
  }

  object CostFunction1 {
    final case class Constant(c: Cost) extends CostFunction1[Any] {
      override def cost(x: Any): Cost = c
    }
    val Null: CostFunction1[Any] = Constant(0)
  }

  abstract class CostFunction2[-X, -Y] {
    def cost(x: X, y: Y): Cost
  }

  object CostFunction2 {
    final case class Constant(c: Cost) extends CostFunction2[Any, Any] {
      override def cost(x: Any, y: Any): Cost = c
    }
    val Null: CostFunction2[Any, Any] = Constant(0)
  }

  abstract class CostFunction3[-X, -Y, -Z] {
    def cost(x: Y, y: Y, z: Z): Cost
  }

  object CostFunction3 {
    final case class Constant(c: Cost) extends CostFunction3[Any, Any, Any] {
      override def cost(x: Any, y: Any, z: Any): Cost = c
    }
    val Null: CostFunction3[Any, Any, Any] = Constant(0)
  }

  val Empty = new EmptyModel

  class EmptyModel extends CostModel {
    override val AddNumeric: CostFunction2[Numeric, Numeric] = CostFunction2.Null
    override val SubNumeric: CostFunction2[Numeric, Numeric] = CostFunction2.Null
    override val MulNumeric: CostFunction3[Numeric, Numeric, Numeric] = CostFunction3.Null
    override val DivNumeric: CostFunction3[Numeric, Numeric, Numeric] = CostFunction3.Null
    override val BRoundNumeric: CostFunction2[Int, Numeric] = CostFunction2.Null
    override val BCastNumeric: CostFunction3[Numeric, Numeric, Numeric] = CostFunction3.Null
    override val BShiftNumeric: CostFunction2[Int, Numeric] = CostFunction2.Null
    override val BAddInt64: CostFunction2[Int64, Int64] = CostFunction2.Null
    override val BSubInt64: CostFunction2[Int64, Int64] = CostFunction2.Null
    override val BMulInt64: CostFunction2[Int64, Int64] = CostFunction2.Null
    override val BDivInt64: CostFunction2[Int64, Int64] = CostFunction2.Null
    override val BModInt64: CostFunction2[Int64, Int64] = CostFunction2.Null
    override val BExpInt64: CostFunction2[Int64, Int64] = CostFunction2.Null
    override val BInt64ToNumeric: CostFunction2[Numeric, Int64] = CostFunction2.Null
    override val BNumericToInt64: CostFunction1[Numeric] = CostFunction1.Null
    override val BTextMapToList: CostFunction1[TextMap] = CostFunction1.Null
    override val BGenMapInsert: CostFunction3[Value, Value, GenMap] = CostFunction3.Null
    override val BGenMapLookup: CostFunction2[Value, GenMap] = CostFunction2.Null
    override val BGenMapDelete: CostFunction2[Value, GenMap] = CostFunction2.Null
    override val BGenMapToList: CostFunction1[GenMap] = CostFunction1.Null
    override val BGenMapKeys: CostFunction1[GenMap] = CostFunction1.Null
    override val BGenMapValues: CostFunction1[GenMap] = CostFunction1.Null
    override val BGenMapSize: CostFunction1[GenMap] = CostFunction1.Null
    override val BAppendText: CostFunction2[Text, Text] = CostFunction2.Null
    override val BInt64ToText: CostFunction1[Int64] = CostFunction1.Null
    override val BNumericToText: CostFunction1[Numeric] = CostFunction1.Null
    override val BTimestampToText: CostFunction1[Timestamp] = CostFunction1.Null
    override val BPartyToText: CostFunction1[Party] = CostFunction1.Null
    override val BContractIdToText: CostFunction1[ContractId] = CostFunction1.Null
    override val BCodePointsToText: CostFunction1[FrontStack[Long]] = CostFunction1.Null
    override val BTextToParty: CostFunction1[Text] = CostFunction1.Null
    override val BTextToInt64: CostFunction1[Text] = CostFunction1.Null
    override val BTextToNumeric: CostFunction2[Int, Text] = CostFunction2.Null
    override val BTextToCodePoints: CostFunction1[Text] = CostFunction1.Null
    override val BSHA256Text: CostFunction1[Text] = CostFunction1.Null
    override val BKECCAK256Text: CostFunction1[Text] = CostFunction1.Null
    override val BSECP256K1Bool: CostFunction1[Text] = CostFunction1.Null
    override val BDecodeHex: CostFunction1[Text] = CostFunction1.Null
    override val BEncodeHex: CostFunction1[Text] = CostFunction1.Null
    override val BTextToContractId: CostFunction1[Text] = CostFunction1.Null
    override val BDateToUnixDays: CostFunction1[Date] = CostFunction1.Null
    override val BExplodeText: CostFunction1[Text] = CostFunction1.Null
    override val BTimestampToUnixMicroseconds: CostFunction1[Timestamp] = CostFunction1.Null
    override val BDateToText: CostFunction1[Date] = CostFunction1.Null
    override val BUnixDaysToDate: CostFunction1[Int64] = CostFunction1.Null
    override val BUnixMicrosecondsToTimestamp: CostFunction1[Int64] = CostFunction1.Null
    override val BEqual: CostFunction2[Value, Value] = CostFunction2.Null
    override val BLess: CostFunction2[Value, Value] = CostFunction2.Null
    override val BLessEq: CostFunction2[Value, Value] = CostFunction2.Null
    override val BGreater: CostFunction2[Value, Value] = CostFunction2.Null
    override val BGreaterEq: CostFunction2[Value, Value] = CostFunction2.Null
    override val BCoerceContractId: CostFunction1[ContractId] = CostFunction1.Null

    override val KontStackIncrease: CostFunction1[Int] = CostFunction1.Null
    override val EnvIncrease: CostFunction1[Int] = CostFunction1.Null
  }

}
