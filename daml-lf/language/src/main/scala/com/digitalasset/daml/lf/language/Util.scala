// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.language

import com.digitalasset.daml.lf.data.{Decimal, ImmArray}
import com.digitalasset.daml.lf.data.Ref.TypeConName
import com.digitalasset.daml.lf.language.Ast._

import scala.annotation.tailrec

object Util {

  object TTyConApp {
    def apply(con: TypeConName, args: ImmArray[Type]): Type =
      args.foldLeft[Type](TTyCon(con))((typ, arg) => TApp(typ, arg))
    def unapply(typ: Type): Option[(TypeConName, ImmArray[Type])] = {
      @tailrec
      def go(typ: Type, targs: List[Type]): Option[(TypeConName, ImmArray[Type])] =
        typ match {
          case TApp(tfun, targ) => go(tfun, targ :: targs)
          case TTyCon(con) => Some((con, ImmArray(targs)))
          case _ => None
        }
      go(typ, Nil)
    }
  }

  object TFun extends ((Type, Type) => Type) {
    def apply(targ: Type, tres: Type) =
      TApp(TApp(TBuiltin(BTArrow), targ), tres)
  }

  class ParametricType1(bType: BuiltinType) {
    val cons = TBuiltin(bType)
    def apply(typ: Type): Type =
      TApp(cons, typ)
    def unapply(typ: TApp): Option[Type] = typ match {
      case TApp(`cons`, elemType) => Some(elemType)
      case _ => None
    }
  }

  val TUnit = TBuiltin(BTUnit)
  val TBool = TBuiltin(BTBool)
  val TInt64 = TBuiltin(BTInt64)
  val TText = TBuiltin(BTText)
  val TTimestamp = TBuiltin(BTTimestamp)
  val TDate = TBuiltin(BTDate)
  val TParty = TBuiltin(BTParty)
  val TAnyTemplate = TBuiltin(BTAnyTemplate)

  val TNumeric = new ParametricType1(BTNumeric)
  val TList = new ParametricType1(BTList)
  val TOptional = new ParametricType1(BTOptional)
  val TMap = new ParametricType1(BTMap)
  val TUpdate = new ParametricType1(BTUpdate)
  val TScenario = new ParametricType1(BTScenario)
  val TContractId = new ParametricType1(BTContractId)

  val EUnit = EPrimCon(PCUnit)
  val ETrue = EPrimCon(PCTrue)
  val EFalse = EPrimCon(PCFalse)

  def EBool(b: Boolean): EPrimCon = if (b) ETrue else EFalse

  val TParties = TList(TParty)
  val TDecimalScale = TNat(Decimal.scale)
  val TDecimal = TNumeric(TDecimalScale)

}
