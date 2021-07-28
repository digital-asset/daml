// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation
package iterable

import com.daml.lf.language.Ast._
import com.daml.lf.validation.Util._

private[validation] object ExprIterable {
  that =>

  private[iterable] def iterator(x: Expr): Iterator[Expr] = {
    x match {
      case EVar(_) | EBuiltin(_) | EPrimCon(_) | EPrimLit(_) | EVal(_) | EEnumCon(_, _) |
          // stupid formatter !
          ETypeRep(_) | EExperimental(_, _) =>
        Iterator.empty
      case ELocation(_, expr) =>
        Iterator(expr)
      case ERecCon(tycon @ _, fields) =>
        fields.values
      case ERecProj(tycon @ _, field @ _, record) =>
        Iterator(record)
      case ERecUpd(tycon @ _, field @ _, record, update) =>
        Iterator(record, update)
      case EVariantCon(tycon @ _, variant @ _, arg) =>
        Iterator(arg)
      case EStructCon(fields) =>
        fields.values
      case EStructProj(field @ _, struct) =>
        Iterator(struct)
      case EStructUpd(field @ _, struct, update) =>
        Iterator(struct, update)
      case EApp(fun, arg) =>
        Iterator(fun, arg)
      case ETyApp(expr, typ @ _) =>
        Iterator(expr)
      case EAbs(binder @ _, body, ref @ _) =>
        Iterator(body)
      case ETyAbs(binder @ _, body) =>
        Iterator(body)
      case ECase(scrut, alts) =>
        Iterator(scrut) ++ alts.iterator.map(_.expr)
      case ELet(binding, body) =>
        Iterator(binding.bound, body)
      case ENil(_) => Iterator.empty
      case ECons(typ @ _, front, tail) =>
        front.iterator ++ Iterator(tail)
      case EUpdate(update) =>
        iterator(update)
      case EScenario(scenario) =>
        iterator(scenario)
      case ENone(typ @ _) => Iterator.empty
      case ESome(typ @ _, body) =>
        Iterator(body)
      case EToAny(ty @ _, body) =>
        Iterator(body)
      case EFromAny(ty @ _, body) =>
        Iterator(body)
      case EThrow(returnType @ _, exceptionType @ _, exception) =>
        Iterator(exception)
      case EToAnyException(typ @ _, value) =>
        Iterator(value)
      case EFromAnyException(typ @ _, value) =>
        Iterator(value)
      case ETypeRepGeneric(kind @ _, ty @ _) => Iterator.empty
      case ETypeRepGenericApp(_, _) => Iterator.empty
    }
  }

  private[iterable] def iterator(x: Update): Iterator[Expr] = {
    x match {
      case UpdatePure(typ @ _, expr) =>
        Iterator(expr)
      case UpdateBlock(bindings, body) =>
        bindings.iterator.map(_.bound) ++ Iterator(body)
      case UpdateCreate(templateId @ _, arg) =>
        Iterator(arg)
      case UpdateFetch(templateId @ _, contractId) =>
        Iterator(contractId)
      case UpdateExercise(templateId @ _, choice @ _, cid, arg) =>
        Iterator(cid, arg)
      case UpdateExerciseByKey(templateId @ _, choice @ _, key, arg) =>
        Iterator(key, arg)
      case UpdateGetTime => Iterator.empty
      case UpdateFetchByKey(rbk) =>
        Iterator(rbk.key)
      case UpdateLookupByKey(rbk) =>
        Iterator(rbk.key)
      case UpdateEmbedExpr(typ @ _, body) =>
        Iterator(body)
      case UpdateTryCatch(typ @ _, body, binder @ _, handler) =>
        Iterator(body, handler)
    }
  }

  private[iterable] def iterator(x: Scenario): Iterator[Expr] = {
    x match {
      case ScenarioPure(typ @ _, expr) =>
        Iterator(expr)
      case ScenarioBlock(bindings, body) =>
        bindings.iterator.map(_.bound) ++ Iterator(body)
      case ScenarioCommit(party, update, retType @ _) =>
        Iterator(party, update)
      case ScenarioMustFailAt(party, update, retType @ _) =>
        Iterator(party, update)
      case ScenarioPass(relTime) =>
        Iterator(relTime)
      case ScenarioGetTime => Iterator.empty
      case ScenarioGetParty(name) =>
        Iterator(name)
      case ScenarioEmbedExpr(typ @ _, body) =>
        Iterator(body)
    }
  }

  private[iterable] def iterator(x: Definition): Iterator[Expr] =
    x match {
      case DTypeSyn(params @ _, typ @ _) => Iterator.empty
      case DDataType(serializable @ _, params @ _, dataCons @ _) => Iterator.empty
      case DValue(typ @ _, noPartyLiterals @ _, body, isTest @ _) =>
        Iterator(body)
    }

  private[iterable] def iterator(x: Template): Iterator[Expr] =
    x match {
      case Template(param @ _, precond, signatories, agreementText, choices, observers, key) =>
        Iterator(precond, signatories, agreementText) ++
          choices.values.iterator.flatMap(iterator(_)) ++
          Iterator(observers) ++
          key.iterator.flatMap(iterator(_))
    }

  private[iterable] def iterator(x: TemplateChoice): Iterator[Expr] =
    x match {
      case TemplateChoice(
            name @ _,
            consuming @ _,
            controllers,
            observers,
            selfBinder @ _,
            binder @ _,
            returnType @ _,
            update,
          ) =>
        Iterator(controllers) ++
          observers.iterator ++
          Iterator(update)
    }

  private[iterable] def iterator(x: TemplateKey): Iterator[Expr] =
    x match {
      case TemplateKey(typ @ _, body, maintainers) =>
        Iterator(body, maintainers)
    }

  def apply(expr: Expr): Iterable[Expr] =
    new Iterable[Expr] {
      override def iterator: Iterator[Expr] = ExprIterable.iterator(expr)
    }

  def apply(template: Template): Iterable[Expr] =
    new Iterable[Expr] {
      override def iterator: Iterator[Expr] = ExprIterable.iterator(template)
    }

  def apply(module: Module): Iterable[Expr] =
    new Iterable[Expr] {
      override def iterator: Iterator[Expr] =
        module.definitions.values.iterator.flatMap(ExprIterable.iterator(_)) ++
          module.templates.values.iterator.flatMap(ExprIterable.iterator(_))
    }
}
