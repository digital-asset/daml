// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast._
import com.daml.lf.validation.traversable.ExprTraversable

private[validation] object PartyLiterals {

  @throws[EForbiddenPartyLiterals]
  def checkModule(world: World, pkgId: PackageId, module: Module): Unit =
    module.definitions.foreach {
      case (defName, DDataType(_, _, DataRecord(fields @ _, Some(template)))) =>
        def context = ContextDefValue(pkgId, module.name, defName)
        ExprTraversable(template).foreach(checkExpr(world, context, _))
      case (defName, DValue(typ @ _, noPartyLiterals, body, isTest @ _)) =>
        def context = ContextDefValue(pkgId, module.name, defName)
        if (noPartyLiterals)
          checkExpr(world, context, body)
        else if (module.featureFlags.forbidPartyLiterals)
          throw EForbiddenPartyLiterals(context, ValRefWithPartyLiterals(context.ref))
      case _ =>
    }

  private def checkExpr(world: World, context: => Context, expr: Expr): Unit =
    expr match {
      case EPrimLit(party: PLParty) =>
        throw EForbiddenPartyLiterals(context, PartyLiteral(party.value))
      case EVal(valRef) if !world.lookupValue(context, valRef).noPartyLiterals =>
        throw EForbiddenPartyLiterals(context, ValRefWithPartyLiterals(valRef))
      case otherwise =>
        ExprTraversable(otherwise).foreach(checkExpr(world, context, _))
    }

}
