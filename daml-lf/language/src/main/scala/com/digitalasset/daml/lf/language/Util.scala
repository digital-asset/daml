// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

import com.daml.lf.data.{Decimal, ImmArray}
import com.daml.lf.data.Ref.TypeConName
import com.daml.lf.language.Ast._

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

  class ParametricType2(bType: BuiltinType) {
    val cons = TBuiltin(bType)
    def apply(typ1: Type, typ2: Type): Type =
      TApp(TApp(cons, typ1), typ2)
    def unapply(typ: TApp): Option[(Type, Type)] = typ match {
      case TApp(TApp(`cons`, type1), type2) => Some(type1 -> type2)
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
  val TAny = TBuiltin(BTAny)
  val TTypeRep = TBuiltin(BTTypeRep)

  val TNumeric = new ParametricType1(BTNumeric)
  val TList = new ParametricType1(BTList)
  val TOptional = new ParametricType1(BTOptional)
  val TTextMap = new ParametricType1(BTTextMap)
  val TGenMap = new ParametricType2(BTGenMap)
  val TUpdate = new ParametricType1(BTUpdate)
  val TScenario = new ParametricType1(BTScenario)
  val TContractId = new ParametricType1(BTContractId)

  val TParties = TList(TParty)
  val TDecimalScale = TNat(Decimal.scale)
  val TDecimal = TNumeric(TDecimalScale)

  val EUnit = EPrimCon(PCUnit)
  val ETrue = EPrimCon(PCTrue)
  val EFalse = EPrimCon(PCFalse)

  def EBool(b: Boolean): EPrimCon = if (b) ETrue else EFalse

  val CPUnit = CPPrimCon(PCUnit)
  val CPTrue = CPPrimCon(PCTrue)
  val CPFalse = CPPrimCon(PCFalse)

}
