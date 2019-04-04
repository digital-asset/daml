// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.lfpackage

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.TypeConName
import com.digitalasset.daml.lf.lfpackage.Ast._

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
    def apply(typ: Type): Type =
      TApp(TBuiltin(bType), typ)
    def unapply(typ: TApp): Option[Type] = typ match {
      case TApp(TBuiltin(`bType`), elemType) => Some(elemType)
      case _ => None
    }
  }

  val TList = new ParametricType1(BTList)

  val TOptional = new ParametricType1(BTOptional)

  val TMap = new ParametricType1(BTMap)

  val TUpdate = new ParametricType1(BTUpdate)

  val TScenario = new ParametricType1(BTScenario)

  val TContractId = new ParametricType1(BTContractId)

}
