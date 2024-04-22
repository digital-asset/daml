// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation.iterable

import com.daml.lf.language.Ast._
import com.daml.lf.validation.Util._

private[validation] object TypeIterable {
  that =>

  private def toType(tyCon: TypeConApp): Type =
    (tyCon.args.iterator foldLeft (TTyCon(tyCon.tycon): Type))(TApp)

  private[validation] def iterator(typ: Type): Iterator[Type] =
    typ match {
      case TSynApp(_, args) =>
        args.iterator
      case TVar(_) | TTyCon(_) | TBuiltin(_) | TNat(_) =>
        Iterator.empty
      case TApp(tyfun, arg) =>
        Iterator(tyfun, arg)
      case TForall(binder @ _, body) =>
        Iterator(body)
      case TStruct(fields) =>
        fields.values
    }

  private[validation] def iterator(expr0: Expr): Iterator[Type] = {
    expr0 match {
      case ERecCon(tycon, fields @ _) =>
        Iterator(toType(tycon)) ++
          fields.values.flatMap(iterator(_))
      case ERecProj(tycon, field @ _, record) =>
        Iterator(toType(tycon)) ++
          iterator(record)
      case ERecUpd(tycon, field @ _, record, update) =>
        Iterator(toType(tycon)) ++
          iterator(record) ++ iterator(update)
      case EVariantCon(tycon, variant @ _, arg) =>
        Iterator(toType(tycon)) ++
          iterator(arg)
      case ETyApp(expr, typ) =>
        iterator(expr) ++ Iterator(typ)
      case EAbs((boundVarName @ _, boundVarType), body, ref @ _) =>
        Iterator(boundVarType) ++ iterator(body)
      case ELet(binding, body) =>
        iterator(binding) ++ iterator(body)
      case EEnumCon(tyConName, _) =>
        Iterator(TTyCon(tyConName))
      case EToAny(typ, expr) =>
        Iterator(typ) ++ iterator(expr)
      case EFromAny(typ, expr) =>
        Iterator(typ) ++ iterator(expr)
      case ETypeRep(tyCon) =>
        Iterator(tyCon)
      case ENil(typ) =>
        Iterator(typ)
      case ECons(typ, front, tail) =>
        Iterator(typ) ++
          front.iterator.flatMap(iterator(_)) ++
          iterator(tail)
      case ENone(typ) =>
        Iterator(typ)
      case ESome(typ, body) =>
        Iterator(typ) ++ iterator(body)
      case EUpdate(u) =>
        iterator(u)
      case EScenario(s) =>
        iterator(s)
      case EThrow(returnType, exceptionType, exception) =>
        Iterator(returnType, exceptionType) ++
          iterator(exception)
      case EToAnyException(typ, value) =>
        Iterator(typ) ++
          iterator(value)
      case EFromAnyException(typ, value) =>
        Iterator(typ) ++
          iterator(value)
      case EToInterface(iface, tpl, value) =>
        Iterator(TTyCon(iface), TTyCon(tpl)) ++ iterator(value)
      case EFromInterface(iface, tpl, value) =>
        Iterator(TTyCon(iface), TTyCon(tpl)) ++ iterator(value)
      case EUnsafeFromInterface(iface, tpl, cid, value) =>
        Iterator(TTyCon(iface), TTyCon(tpl)) ++ iterator(cid) ++ iterator(value)
      case ECallInterface(iface, _, value) =>
        Iterator(TTyCon(iface)) ++ iterator(value)
      case EToRequiredInterface(requiredIfaceId, requiringIfaceId, body) =>
        Iterator(TTyCon(requiredIfaceId), TTyCon(requiringIfaceId)) ++ iterator(body)
      case EFromRequiredInterface(requiredIfaceId, requiringIfaceId, body) =>
        Iterator(TTyCon(requiredIfaceId), TTyCon(requiringIfaceId)) ++ iterator(body)
      case EUnsafeFromRequiredInterface(requiredIfaceId, requiringIfaceId, cid, body) =>
        Iterator(TTyCon(requiredIfaceId), TTyCon(requiringIfaceId)) ++
          iterator(cid) ++ iterator(body)
      case EInterfaceTemplateTypeRep(ifaceId @ _, body) =>
        Iterator(TTyCon(ifaceId)) ++ iterator(body)
      case ESignatoryInterface(ifaceId, body) =>
        Iterator(TTyCon(ifaceId)) ++ iterator(body)
      case EObserverInterface(ifaceId, body) =>
        Iterator(TTyCon(ifaceId)) ++ iterator(body)
      case EViewInterface(ifaceId, expr) =>
        Iterator(TTyCon(ifaceId)) ++
          iterator(expr)
      case EChoiceController(tpl, choiceName @ _, contract, choiceArg) =>
        Iterator(TTyCon(tpl)) ++
          iterator(contract) ++
          iterator(choiceArg)
      case EChoiceObserver(tpl, choiceName @ _, contract, choiceArg) =>
        Iterator(TTyCon(tpl)) ++
          iterator(contract) ++
          iterator(choiceArg)
      case EVar(_) | EVal(_) | EBuiltinFun(_) | EBuiltinCon(_) | EBuiltinLit(_) | EApp(_, _) |
          ECase(_, _) | ELocation(_, _) | EStructCon(_) | EStructProj(_, _) | EStructUpd(_, _, _) |
          ETyAbs(_, _) | EExperimental(_, _) =>
        ExprIterable.iterator(expr0).flatMap(iterator(_))
    }
  }

  private[validation] def iterator(update: Update): Iterator[Type] =
    update match {
      case UpdatePure(typ, expr) =>
        Iterator(typ) ++ iterator(expr)
      case UpdateBlock(bindings, body) =>
        bindings.iterator.flatMap(iterator(_)) ++
          iterator(body)
      case UpdateCreate(templateId, arg) =>
        Iterator(TTyCon(templateId)) ++
          iterator(arg)
      case UpdateCreateInterface(interface, arg) =>
        Iterator(TTyCon(interface)) ++
          iterator(arg)
      case UpdateFetchTemplate(templateId, contractId) =>
        Iterator(TTyCon(templateId)) ++
          iterator(contractId)
      case UpdateSoftFetchTemplate(templateId, contractId) =>
        Iterator(TTyCon(templateId)) ++
          iterator(contractId)
      case UpdateFetchInterface(interface, contractId) =>
        Iterator(TTyCon(interface)) ++
          iterator(contractId)
      case UpdateExercise(templateId, choice @ _, cid, arg) =>
        Iterator(TTyCon(templateId)) ++
          iterator(cid) ++
          iterator(arg)
      case UpdateSoftExercise(templateId, choice @ _, cid, arg) =>
        Iterator(TTyCon(templateId)) ++
          iterator(cid) ++
          iterator(arg)
      case UpdateDynamicExercise(templateId, choice @ _, cid, arg) =>
        Iterator(TTyCon(templateId)) ++
          iterator(cid) ++
          iterator(arg)
      case UpdateExerciseInterface(interface, choice @ _, cid, arg, guard) =>
        Iterator(TTyCon(interface)) ++
          iterator(cid) ++
          iterator(arg) ++
          guard.iterator.flatMap(iterator(_))
      case UpdateExerciseByKey(templateId, choice @ _, key, arg) =>
        Iterator(TTyCon(templateId)) ++
          iterator(key) ++
          iterator(arg)
      case UpdateEmbedExpr(typ, body) =>
        Iterator(typ) ++
          iterator(body)
      case UpdateGetTime | UpdateFetchByKey(_) | UpdateLookupByKey(_) =>
        ExprIterable.iterator(update).flatMap(iterator(_))
      case UpdateTryCatch(typ, body, binder @ _, handler) =>
        Iterator(typ) ++
          iterator(body) ++
          iterator(handler)
    }

  private[validation] def iterator(binding: Binding): Iterator[Type] =
    binding match {
      case Binding(binder @ _, typ, bound) =>
        Iterator(typ) ++ iterator(bound)
    }

  private[validation] def iterator(scenario: Scenario): Iterator[Type] =
    scenario match {
      case ScenarioPure(typ, expr) =>
        Iterator(typ) ++ iterator(expr)
      case ScenarioBlock(bindings, body) =>
        bindings.iterator.flatMap(iterator(_)) ++
          iterator(body)
      case ScenarioCommit(party, update, retType) =>
        iterator(party) ++
          iterator(update) ++
          Iterator(retType)
      case ScenarioMustFailAt(party, update, retType) =>
        iterator(party) ++
          iterator(update) ++
          Iterator(retType)
      case ScenarioEmbedExpr(typ, body) =>
        Iterator(typ) ++
          iterator(body)
      case ScenarioGetTime | ScenarioPass(_) | ScenarioGetParty(_) =>
        ExprIterable.iterator(scenario).flatMap(iterator(_))
    }

  private[validation] def iterator(defn: Definition): Iterator[Type] =
    defn match {
      case DTypeSyn(params @ _, typ) =>
        Iterator(typ)
      case DDataType(serializable @ _, params @ _, DataRecord(fields)) =>
        fields.values
      case DDataType(serializable @ _, params @ _, DataVariant(variants)) =>
        variants.values
      case DDataType(serializable @ _, params @ _, DataEnum(values @ _)) =>
        Iterator.empty
      case DDataType(serializable @ _, params @ _, DataInterface) =>
        Iterator.empty
      case DValue(typ, body, isTest @ _) =>
        Iterator(typ) ++ iterator(body)

    }

  private[validation] def iterator(x: Template): Iterator[Type] =
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
        iterator(precond) ++
          iterator(signatories) ++
          choices.values.flatMap(iterator(_)) ++
          iterator(observers) ++
          key.iterator.flatMap(iterator(_)) ++
          implements.values.flatMap(iterator(_))
    }

  private[validation] def iterator(choice: TemplateChoice): Iterator[Type] =
    choice match {
      case TemplateChoice(
            name @ _,
            consuming @ _,
            controllers,
            observers,
            authorizers,
            selfBinder @ _,
            (boundedVarName @ _, boundedVarType),
            retType,
            update,
          ) =>
        iterator(controllers) ++
          observers.iterator.flatMap(iterator(_)) ++
          authorizers.iterator.flatMap(iterator(_)) ++
          iterator(update) ++
          Iterator(boundedVarType) ++
          Iterator(retType)
    }

  private[validation] def iterator(key: TemplateKey): Iterator[Type] =
    key match {
      case TemplateKey(typ, body, maintainers) =>
        Iterator(typ) ++
          iterator(body) ++
          iterator(maintainers)
    }

  private[validation] def iterator(impl: TemplateImplements): Iterator[Type] =
    impl match {
      case TemplateImplements(interface, body) =>
        Iterator(TTyCon(interface)) ++ iterator(body)
    }

  private[validation] def iterator(iiBody: InterfaceInstanceBody): Iterator[Type] =
    iiBody match {
      case InterfaceInstanceBody(methods, view) =>
        methods.values.iterator.flatMap(iterator) ++ iterator(view)
    }

  private[validation] def iterator(method: InterfaceInstanceMethod): Iterator[Type] =
    method match {
      case InterfaceInstanceMethod(name @ _, value) =>
        iterator(value)
    }

  private[validation] def iterator(coImpl: InterfaceCoImplements): Iterator[Type] =
    coImpl match {
      case InterfaceCoImplements(template, body) =>
        Iterator(TTyCon(template)) ++ iterator(body)
    }

  private[validation] def iterator(interface: DefInterface): Iterator[Type] =
    interface match {
      case DefInterface(requires, _, choices, methods, view, coImplements) =>
        requires.iterator.map(TTyCon) ++
          choices.values.iterator.flatMap(iterator) ++
          methods.values.iterator.flatMap(iterator) ++
          iterator(view) ++
          coImplements.values.flatMap(iterator)
    }

  private[validation] def iterator(imethod: InterfaceMethod): Iterator[Type] =
    imethod match {
      case InterfaceMethod(name @ _, retType) =>
        Iterator(retType)
    }

  def apply(typ: Type): Iterable[Type] =
    new Iterable[Type] {
      override def iterator = that.iterator(typ)
    }

  def apply(expr: Expr): Iterable[Type] =
    new Iterable[Type] {
      override def iterator = that.iterator(expr)
    }

  def apply(module: Module): Iterable[Type] =
    new Iterable[Type] {
      override def iterator: Iterator[Type] =
        module.definitions.values.iterator.flatMap(that.iterator(_)) ++
          module.interfaces.values.iterator.flatMap(that.iterator(_)) ++
          module.templates.values.iterator.flatMap(that.iterator(_))
    }
}
