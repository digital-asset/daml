// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
      case EToInterface(iface @ _, tpl @ _, value) =>
        Iterator(value)
      case EFromInterface(iface @ _, tpl @ _, value) =>
        Iterator(value)
      case EUnsafeFromInterface(iface @ _, tpl @ _, cid, value) =>
        Iterator(cid, value)
      case ECallInterface(iface @ _, method @ _, value) =>
        Iterator(value)
      case EToRequiredInterface(requiredIface @ _, requiringIface @ _, body) =>
        iterator(body)
      case EFromRequiredInterface(requiredIface @ _, requiringIface @ _, body) =>
        iterator(body)
      case EUnsafeFromRequiredInterface(requiredIface @ _, requiringIface @ _, cid, body) =>
        Iterator(cid, body)
      case EInterfaceTemplateTypeRep(iface @ _, body) =>
        iterator(body)
      case ESignatoryInterface(iface @ _, body) =>
        iterator(body)
      case EViewInterface(ifaceId @ _, expr) =>
        iterator(expr)
      case EObserverInterface(iface @ _, body) =>
        iterator(body)
      case EChoiceController(tpl @ _, choiceName @ _, contract, choiceArg) =>
        Iterator(contract, choiceArg)
      case EChoiceObserver(tpl @ _, choiceName @ _, contract, choiceArg) =>
        Iterator(contract, choiceArg)
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
      case UpdateCreateInterface(interface @ _, arg) =>
        Iterator(arg)
      case UpdateFetchTemplate(templateId @ _, contractId) =>
        Iterator(contractId)
      case UpdateSoftFetchTemplate(templateId @ _, contractId) =>
        Iterator(contractId)
      case UpdateFetchInterface(interface @ _, contractId) =>
        Iterator(contractId)
      case UpdateExercise(templateId @ _, choice @ _, cid, arg) =>
        Iterator(cid, arg)
      case UpdateSoftExercise(templateId @ _, choice @ _, cid, arg) =>
        Iterator(cid, arg)
      case UpdateDynamicExercise(templateId @ _, choice @ _, cid, arg) =>
        Iterator(cid, arg)
      case UpdateExerciseInterface(interface @ _, choice @ _, cid, arg, guard) =>
        Iterator(cid, arg) ++ guard.iterator
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
      case DValue(typ @ _, body, isTest @ _) =>
        Iterator(body)
    }

  private[iterable] def iterator(x: Template): Iterator[Expr] =
    x match {
      case Template(
            param @ _,
            precond,
            signatories,
            choices,
            observers,
            key,
            implements,
          ) =>
        Iterator(precond, signatories) ++
          choices.values.iterator.flatMap(iterator(_)) ++
          Iterator(observers) ++
          key.iterator.flatMap(iterator(_)) ++
          implements.values.iterator.flatMap(iterator(_))
    }

  private[iterable] def iterator(x: TemplateChoice): Iterator[Expr] =
    x match {
      case TemplateChoice(
            name @ _,
            consuming @ _,
            controllers,
            observers,
            authorizers,
            selfBinder @ _,
            binder @ _,
            returnType @ _,
            update,
          ) =>
        Iterator(controllers) ++
          observers.iterator ++
          authorizers.iterator ++
          Iterator(update)
    }

  private[iterable] def iterator(x: TemplateKey): Iterator[Expr] =
    x match {
      case TemplateKey(typ @ _, body, maintainers) =>
        Iterator(body, maintainers)
    }

  private[iterable] def iterator(x: TemplateImplements): Iterator[Expr] =
    x match {
      case TemplateImplements(
            interface @ _,
            body,
          ) =>
        iterator(body)
    }

  private[iterable] def iterator(x: InterfaceInstanceBody): Iterator[Expr] =
    x match {
      case InterfaceInstanceBody(
            methods,
            view,
          ) =>
        methods.values.iterator.flatMap(iterator(_)) ++
          iterator(view)
    }

  private[iterable] def iterator(x: InterfaceInstanceMethod): Iterator[Expr] =
    x match {
      case InterfaceInstanceMethod(name @ _, value) =>
        Iterator(value)
    }

  private[iterable] def iterator(x: DefException): Iterator[Expr] =
    x match {
      case DefException(
            message
          ) =>
        Iterator(message)
    }

  private[iterable] def iterator(x: DefInterface): Iterator[Expr] =
    x match {
      case DefInterface(
            requires @ _,
            param @ _,
            choices,
            methods @ _,
            coImplements,
            view @ _,
          ) =>
        choices.values.iterator.flatMap(iterator(_)) ++
          coImplements.values.iterator.flatMap(iterator(_))
    }

  private[iterable] def iterator(x: InterfaceCoImplements): Iterator[Expr] =
    x match {
      case InterfaceCoImplements(
            template @ _,
            body,
          ) =>
        iterator(body)
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
          module.exceptions.values.iterator.flatMap(ExprIterable.iterator(_)) ++
          module.interfaces.values.iterator.flatMap(ExprIterable.iterator(_)) ++
          module.templates.values.iterator.flatMap(ExprIterable.iterator(_))
    }
}
