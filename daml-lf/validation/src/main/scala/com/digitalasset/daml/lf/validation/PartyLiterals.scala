// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast._
import com.daml.lf.language.PackageInterface
import com.daml.lf.validation.iterable.ExprIterable

private[validation] object PartyLiterals {

  import Util.handleLookup

  @throws[EForbiddenPartyLiterals]
  def checkModule(interface: PackageInterface, pkgId: PackageId, module: Module): Unit = {
    module.definitions.foreach {
      case (defName, DValue(typ @ _, noPartyLiterals, body, isTest @ _)) =>
        def context = ContextDefValue(pkgId, module.name, defName)
        if (noPartyLiterals)
          checkExpr(interface, context, body)
        else if (module.featureFlags.forbidPartyLiterals)
          throw EForbiddenPartyLiterals(context, ValRefWithPartyLiterals(context.ref))
      case _ =>
    }
    module.templates.foreach { case (defName, template) =>
      def context = ContextDefValue(pkgId, module.name, defName)
      ExprIterable(template).foreach(checkExpr(interface, context, _))
    }
  }

  private def checkExpr(interface: PackageInterface, context: => Context, expr: Expr): Unit =
    expr match {
      case EPrimLit(party: PLParty) =>
        throw EForbiddenPartyLiterals(context, PartyLiteral(party.value))
      case EVal(valRef) if !handleLookup(context, interface.lookupValue(valRef)).noPartyLiterals =>
        throw EForbiddenPartyLiterals(context, ValRefWithPartyLiterals(valRef))
      case otherwise =>
        ExprIterable(otherwise).foreach(checkExpr(interface, context, _))
    }

}
