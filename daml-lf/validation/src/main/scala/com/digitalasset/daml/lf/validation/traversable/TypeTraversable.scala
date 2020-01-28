// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation
package traversable

import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.validation.Util._

private[validation] object TypeTraversable {
  that =>

  private def toType(tyCon: TypeConApp): Type =
    ((TTyCon(tyCon.tycon): Type) /: tyCon.args.iterator)(TApp)

  private[validation] def foreach[U](typ: Type, f: Type => U): Unit =
    typ match {
      case TSynApp(_, args) =>
        args.iterator.foreach(f)
      case TVar(_) | TTyCon(_) | TBuiltin(_) | TNat(_) =>
      case TApp(tyfun, arg) =>
        f(tyfun)
        f(arg)
        ()
      case TForall(binder @ _, body) =>
        f(body)
        ()
      case TStruct(fields) =>
        fields.values.foreach(f)
    }

  private[validation] def foreach[U](expr0: Expr, f: Type => U): Unit = {
    expr0 match {
      case ERecCon(tycon, fields @ _) =>
        f(toType(tycon))
        fields.values.foreach(foreach(_, f))
      case ERecProj(tycon, field @ _, record) =>
        f(toType(tycon))
        foreach(record, f)
      case ERecUpd(tycon, field @ _, record, update) =>
        f(toType(tycon))
        foreach(record, f)
        foreach(update, f)
      case EVariantCon(tycon, variant @ _, arg) =>
        f(toType(tycon))
        foreach(arg, f)
      case ETyApp(expr, typ) =>
        foreach(expr, f)
        f(typ)
      case EAbs((boundVarName @ _, boundVarType), body, ref @ _) =>
        f(boundVarType)
        foreach(body, f)
      case ELet(binding, body) =>
        foreach(binding, f)
        foreach(body, f)
      case EEnumCon(tyConName, _) =>
        f(TTyCon(tyConName))
      case EToAny(typ, expr) =>
        f(typ)
        foreach(expr, f)
      case EFromAny(typ, expr) =>
        f(typ)
        foreach(expr, f)
      case ETypeRep(tyCon) =>
        f(tyCon)
      case ENil(typ) =>
        f(typ)
      case ECons(typ, front, tail) =>
        f(typ)
        front.iterator.foreach(foreach(_, f))
        foreach(tail, f)
      case ENone(typ) =>
        f(typ)
      case ESome(typ, body) =>
        f(typ)
        foreach(body, f)
      case EUpdate(u) =>
        foreach(u, f)
      case EScenario(s) =>
        foreach(s, f)
      case EVar(_) | EVal(_) | EBuiltin(_) | EPrimCon(_) | EPrimLit(_) | EApp(_, _) | ECase(_, _) |
          ELocation(_, _) | EStructCon(_) | EStructProj(_, _) | EStructUpd(_, _, _) |
          ETyAbs(_, _) =>
        ExprTraversable.foreach(expr0, foreach(_, f))
    }
    ()
  }

  private[validation] def foreach[U](update: Update, f: Type => U): Unit =
    update match {
      case UpdatePure(typ, expr) =>
        f(typ)
        foreach(expr, f)
      case UpdateBlock(bindings, body) =>
        bindings.iterator.foreach(foreach(_, f))
        foreach(body, f)
      case UpdateCreate(templateId, arg) =>
        f(TTyCon(templateId))
        foreach(arg, f)
      case UpdateFetch(templateId, contractId) =>
        f(TTyCon(templateId))
        foreach(contractId, f)
      case UpdateExercise(templateId, choice @ _, cid, actors, arg) =>
        f(TTyCon(templateId))
        foreach(cid, f)
        actors.foreach(foreach(_, f))
        foreach(arg, f)
      case UpdateEmbedExpr(typ, body) =>
        f(typ)
        foreach(body, f)
      case UpdateGetTime | UpdateFetchByKey(_) | UpdateLookupByKey(_) =>
        ExprTraversable.foreach(update, foreach(_, f))
    }

  private[validation] def foreach[U](binding: Binding, f: Type => U): Unit =
    binding match {
      case Binding(binder @ _, typ, bound) =>
        f(typ)
        foreach(bound, f)
    }

  private[validation] def foreach[U](scenario: Scenario, f: Type => U): Unit =
    scenario match {
      case ScenarioPure(typ, expr) =>
        f(typ)
        foreach(expr, f)
      case ScenarioBlock(bindings, body) =>
        bindings.foreach(foreach(_, f))
        foreach(body, f)
      case ScenarioCommit(party, update, retType) =>
        foreach(party, f)
        foreach(update, f)
        f(retType)
        ()
      case ScenarioMustFailAt(party, update, retType) =>
        foreach(party, f)
        foreach(update, f)
        f(retType)
        ()
      case ScenarioEmbedExpr(typ, body) =>
        f(typ)
        foreach(body, f)
      case ScenarioGetTime | ScenarioPass(_) | ScenarioGetParty(_) =>
        ExprTraversable.foreach(scenario, foreach(_, f))
    }

  private[validation] def foreach[U](defn: Definition, f: Type => U): Unit =
    defn match {
      case DTypeSyn(params @ _, typ) =>
        f(typ)
        ()
      case DDataType(serializable @ _, params @ _, DataRecord(fields, template)) =>
        fields.values.foreach(f)
        template.foreach(foreach(_, f))
      case DDataType(serializable @ _, params @ _, DataVariant(variants)) =>
        variants.values.foreach(f)
      case DDataType(serializable @ _, params @ _, DataEnum(values @ _)) =>
      case DValue(typ, noPartyLiterals @ _, body, isTest @ _) =>
        f(typ)
        foreach(body, f)
    }

  private[validation] def foreach[U](x: Template, f: Type => U): Unit =
    x match {
      case Template(param @ _, precond, signatories, agreementText, choices, observers, key) =>
        foreach(precond, f)
        foreach(signatories, f)
        foreach(agreementText, f)
        choices.values.foreach(foreach(_, f))
        foreach(observers, f)
        key.foreach(foreach(_, f))
    }

  private[validation] def foreach[U](choice: TemplateChoice, f: Type => U): Unit =
    choice match {
      case TemplateChoice(
          name @ _,
          consuming @ _,
          controllers,
          selfBinder @ _,
          (boundedVarName @ _, boundedVarType),
          retType,
          update) =>
        foreach(controllers, f)
        foreach(update, f)
        f(boundedVarType)
        f(retType)
        ()
    }

  private[validation] def foreach[U](key: TemplateKey, f: Type => U): Unit =
    key match {
      case TemplateKey(typ, body, maintainers) =>
        f(typ)
        foreach(body, f)
        foreach(maintainers, f)
    }

  def apply(typ: Type): Traversable[Type] =
    new Traversable[Type] {
      def foreach[U](f: Type => U): Unit = that.foreach(typ, f)
    }

  def apply(expr: Expr): Traversable[Type] =
    new Traversable[Type] {
      def foreach[U](f: Type => U): Unit = that.foreach(expr, f)
    }

  def apply(definition: Definition): Traversable[Type] =
    new Traversable[Type] {
      def foreach[U](f: Type => U): Unit = that.foreach(definition, f)
    }

}
