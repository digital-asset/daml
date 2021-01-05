// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation
package traversable

import com.daml.lf.language.Ast._
import com.daml.lf.validation.Util._

private[validation] object ExprTraversable {
  that =>

  private[traversable] def foreach[U](x: Expr, f: Expr => U): Unit = {
    x match {
      case EVar(_) | EBuiltin(_) | EPrimCon(_) | EPrimLit(_) | EVal(_) | EEnumCon(_, _) | ETypeRep(
            _) =>
      case ELocation(_, expr) =>
        f(expr)
      case ERecCon(tycon @ _, fields) =>
        fields.values.foreach(f)
      case ERecProj(tycon @ _, field @ _, record) =>
        f(record)
      case ERecUpd(tycon @ _, field @ _, record, update) =>
        f(record)
        f(update)
      case EVariantCon(tycon @ _, variant @ _, arg) =>
        f(arg)
      case EStructCon(fields) =>
        fields.values.foreach(f)
      case EStructProj(field @ _, struct) =>
        f(struct)
      case EStructUpd(field @ _, struct, update) =>
        f(struct)
        f(update)
      case EApp(fun, arg) =>
        f(fun)
        f(arg)
      case ETyApp(expr, typ @ _) =>
        f(expr)
      case EAbs(binder @ _, body, ref @ _) =>
        f(body)
      case ETyAbs(binder @ _, body) =>
        f(body)
      case ECase(scrut, alts) =>
        f(scrut)
        alts.iterator.foreach(a => f(a.expr))
      case ELet(binding, body) =>
        f(binding.bound)
        f(body)
      case ENil(_) =>
      case ECons(typ @ _, front, tail) =>
        front.iterator.foreach(f)
        f(tail)
      case EUpdate(update) =>
        foreach(update, f)
      case EScenario(scenario) =>
        foreach(scenario, f)
      case ENone(typ @ _) =>
      case ESome(typ @ _, body) =>
        f(body)
      case EToAny(ty @ _, body) =>
        f(body)
      case EFromAny(ty @ _, body) =>
        f(body)
      case EThrow(returnType @ _, exceptionType @ _, exception) =>
        f(exception)
      case EToAnyException(typ @ _, value) =>
        f(value)
      case EFromAnyException(typ @ _, value) =>
        f(value)
    }
    ()
  }

  private[traversable] def foreach[U](x: Update, f: Expr => U): Unit = {
    x match {
      case UpdatePure(typ @ _, expr) =>
        f(expr)
      case UpdateBlock(bindings, body) =>
        bindings.iterator.foreach(b => f(b.bound))
        f(body)
      case UpdateCreate(templateId @ _, arg) =>
        f(arg)
      case UpdateFetch(templateId @ _, contractId) =>
        f(contractId)
      case UpdateExercise(templateId @ _, choice @ _, cid, arg) =>
        f(cid)
        f(arg)
      case UpdateExerciseByKey(templateId @ _, choice @ _, key, arg) =>
        f(key)
        f(arg)
      case UpdateGetTime =>
      case UpdateFetchByKey(rbk) =>
        f(rbk.key)
      case UpdateLookupByKey(rbk) =>
        f(rbk.key)
      case UpdateEmbedExpr(typ @ _, body) =>
        f(body)
      case UpdateTryCatch(typ @ _, body, binder @ _, handler) =>
        f(body)
        f(handler)
    }
    ()
  }

  private[traversable] def foreach[U](x: Scenario, f: Expr => U): Unit = {
    x match {
      case ScenarioPure(typ @ _, expr) =>
        f(expr)
      case ScenarioBlock(bindings, body) =>
        bindings.iterator.foreach(b => f(b.bound))
        f(body)
      case ScenarioCommit(party, update, retType @ _) =>
        f(party)
        f(update)
      case ScenarioMustFailAt(party, update, retType @ _) =>
        f(party)
        f(update)
      case ScenarioPass(relTime) =>
        f(relTime)
      case ScenarioGetTime =>
      case ScenarioGetParty(name) =>
        f(name)
      case ScenarioEmbedExpr(typ @ _, body) =>
        f(body)
    }
    ()
  }

  private[traversable] def foreach[U](x: Definition, f: Expr => U): Unit =
    x match {
      case DTypeSyn(params @ _, typ @ _) =>
      case DDataType(serializable @ _, params @ _, dataCons @ _) =>
      case DValue(typ @ _, noPartyLiterals @ _, body, isTest @ _) =>
        f(body)
        ()
    }

  private[traversable] def foreach[U](x: Template, f: Expr => U): Unit =
    x match {
      case Template(param @ _, precond, signatories, agreementText, choices, observers, key) =>
        f(precond)
        f(signatories)
        f(agreementText)
        choices.values.foreach(foreach(_, f))
        f(observers)
        key.foreach(foreach(_, f))
    }

  private[traversable] def foreach[U](x: TemplateChoice, f: Expr => U): Unit =
    x match {
      case TemplateChoice(
          name @ _,
          consuming @ _,
          controllers,
          observers,
          selfBinder @ _,
          binder @ _,
          returnType @ _,
          update) =>
        f(controllers)
        observers.map(f)
        f(update)
        ()
    }

  private[traversable] def foreach[U](x: TemplateKey, f: Expr => U): Unit =
    x match {
      case TemplateKey(typ @ _, body, maintainers) =>
        f(body)
        f(maintainers)
        ()
    }

  def apply(expr: Expr): Traversable[Expr] =
    new Traversable[Expr] {
      def foreach[U](f: Expr => U): Unit = that.foreach(expr, f)
    }

  def apply(template: Template): Traversable[Expr] =
    new Traversable[Expr] {
      def foreach[U](f: Expr => U): Unit = that.foreach(template, f)
    }

  def apply(module: Module): Traversable[Expr] =
    new Traversable[Expr] {
      def foreach[U](f: Expr => U): Unit = {
        module.definitions.values.foreach(that.foreach(_, f))
        module.templates.values.foreach(that.foreach(_, f))
      }
    }

}
