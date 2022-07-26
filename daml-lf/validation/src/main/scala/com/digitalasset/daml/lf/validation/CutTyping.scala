// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.language.{LanguageVersion, PackageInterface}

private[validation] object CutTyping { // NICK

  def checkModule(pkgInterface: PackageInterface, pkgId: PackageId, mod: Module): Unit = {
    val _ = (pkgInterface, pkgId, mod)
    ()
  }

  case class Env(
      languageVersion: LanguageVersion,
      pkgInterface: PackageInterface,
      ctx: Context,
      tVars: Map[TypeVarName, Kind] = Map.empty,
      eVars: Map[ExprVarName, Type] = Map.empty,
  ) {

    private[lf] def expandTypeSynonyms(typ0: Type): Type = typ0

    def checkKind(kind: Kind): Unit = {
      val _ = kind
      ()
    }

    def kindOf(typ0: Type): Kind = {
      val _ = typ0
      KStar
    }

    def typeOf(e: Expr): Type = {
      val _ = e
      TBuiltin(BTText)
    }
  }

}
