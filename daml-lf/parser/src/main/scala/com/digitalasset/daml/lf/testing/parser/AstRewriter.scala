// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.testing.parser

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._

import scala.{PartialFunction => PF}

private[daml] class AstRewriter(
    typeRule: PF[Type, Type] = PF.empty,
    exprRule: PF[Expr, Expr] = PF.empty,
    identifierRule: PF[Identifier, Identifier] = PF.empty,
    packageIdRule: PF[PackageId, PackageId] = PF.empty,
) {

  import AstRewriter._

  def apply(pkg: Package): Package =
    pkg.copy(modules = pkg.modules.transform((_, x) => apply(x)))

  def apply(module: Module): Module =
    Module(
      name = module.name,
      definitions = module.definitions.transform((_, x) => apply(x)),
      templates = module.templates.transform((_, x) => apply(x)),
      exceptions = module.exceptions.transform((_, x) => apply(x)),
      interfaces = module.interfaces.transform((_, x) => apply(x)),
      featureFlags = module.featureFlags,
    )

  def apply(identifier: Identifier): Identifier =
    if (identifierRule.isDefinedAt(identifier))
      identifierRule(identifier)
    else if (packageIdRule.isDefinedAt(identifier.packageId))
      identifier.copy(packageId = packageIdRule(identifier.packageId))
    else
      identifier

  def apply(x: Type): Type =
    if (typeRule.isDefinedAt(x)) typeRule(x)
    else
      x match {
        case TSynApp(_, _) => throw new RuntimeException("TODO #3616,AstRewriter,TSynApp")
        case TVar(_) | TNat(_) | TBuiltin(_) => x
        case TTyCon(typeCon) =>
          TTyCon(apply(typeCon))
        case TApp(tyfun, arg) =>
          TApp(apply(tyfun), apply(arg))
        case TForall(binder, body) =>
          TForall(binder, apply(body))
        case TStruct(fields) =>
          TStruct(fields.mapValues(apply))
      }

  def apply(nameWithType: (Name, Type)): (Name, Type) = nameWithType match {
    case (name, typ) => (name, apply(typ))
  }

  def apply(x: Expr): Expr =
    if (exprRule.isDefinedAt(x))
      exprRule(x)
    else
      x match {
        case EVar(_) | EBuiltinFun(_) | EBuiltinCon(_) | EBuiltinLit(_) | ETypeRep(_) |
            EExperimental(_, _) =>
          x
        case EVal(ref) =>
          EVal(apply(ref))
        case ELocation(loc, expr) =>
          val newLoc =
            if (packageIdRule.isDefinedAt(loc.packageId))
              loc.copy(packageId = packageIdRule(loc.packageId))
            else
              loc
          ELocation(newLoc, apply(expr))
        case ERecCon(tycon, fields) =>
          ERecCon(
            apply(tycon),
            fields.transform { (_, x) =>
              apply(x)
            },
          )
        case ERecProj(tycon, field, record) =>
          ERecProj(apply(tycon), field, apply(record))
        case ERecUpd(tycon, field, record, update) =>
          ERecUpd(apply(tycon), field, apply(record), apply(update))
        case EVariantCon(tycon, variant, arg) =>
          EVariantCon(apply(tycon), variant, apply(arg))
        case EEnumCon(tyCon, cons) =>
          EEnumCon(apply(tyCon), cons)
        case EStructCon(fields) =>
          EStructCon(fields.transform { (_, x) =>
            apply(x)
          })
        case EStructProj(field, struct) =>
          EStructProj(field, apply(struct))
        case EStructUpd(field, struct, update) =>
          EStructUpd(field, apply(struct), apply(update))
        case EApp(fun, arg) =>
          EApp(apply(fun), apply(arg))
        case ETyApp(expr, typ) =>
          ETyApp(apply(expr), apply(typ))
        case EAbs(binder, body, ref) =>
          EAbs(apply(binder), apply(body), ref)
        case ETyAbs(binder, body) =>
          ETyAbs(binder, apply(body))
        case ECase(scrut, alts) =>
          ECase(apply(scrut), alts.map(apply))
        case ELet(binding, body) =>
          ELet(apply(binding), apply(body))
        case ENil(typ) =>
          ENil(apply(typ))
        case ECons(typ, front, tail) =>
          ECons(apply(typ), front.map(apply), apply(tail))
        case EUpdate(update) =>
          EUpdate(apply(update))
        case EScenario(scenario) =>
          EScenario(apply(scenario))
        case ENone(typ) =>
          ENone(apply(typ))
        case ESome(typ, body) =>
          ESome(apply(typ), apply(body))
        case EToAny(ty, body) =>
          EToAny(apply(ty), apply(body))
        case EFromAny(ty, body) =>
          EFromAny(apply(ty), apply(body))
        case EThrow(returnType, exceptionType, exception) =>
          EThrow(apply(returnType), apply(exceptionType), apply(exception))
        case EFromAnyException(ty, body) =>
          EFromAnyException(apply(ty), apply(body))
        case EToAnyException(typ, body) =>
          EToAnyException(apply(typ), apply(body))
        case EToInterface(iface, tpl, value) =>
          EToInterface(apply(iface), apply(tpl), apply(value))
        case EFromInterface(iface, tpl, value) =>
          EFromInterface(apply(iface), apply(tpl), apply(value))
        case EUnsafeFromInterface(iface, tpl, cid, value) =>
          EUnsafeFromInterface(apply(iface), apply(tpl), apply(cid), apply(value))
        case ECallInterface(iface, method, value) =>
          ECallInterface(apply(iface), method, apply(value))
        case EToRequiredInterface(requiredIfaceId, requiringIfaceId, body) =>
          EToRequiredInterface(apply(requiredIfaceId), apply(requiringIfaceId), apply(body))
        case EFromRequiredInterface(requiredIfaceId, requiringIfaceId, body) =>
          EFromRequiredInterface(apply(requiredIfaceId), apply(requiringIfaceId), apply(body))
        case EUnsafeFromRequiredInterface(requiredIfaceId, requiringIfaceId, cid, body) =>
          EUnsafeFromRequiredInterface(
            apply(requiredIfaceId),
            apply(requiringIfaceId),
            apply(cid),
            apply(body),
          )
        case EInterfaceTemplateTypeRep(ifaceId, body) =>
          EInterfaceTemplateTypeRep(apply(ifaceId), apply(body))
        case ESignatoryInterface(ifaceId, body) =>
          ESignatoryInterface(apply(ifaceId), apply(body))
        case EViewInterface(ifaceId, expr) =>
          EViewInterface(apply(ifaceId), apply(expr))
        case EObserverInterface(ifaceId, body) =>
          EObserverInterface(apply(ifaceId), apply(body))
        case EChoiceController(typeId, choiceName, contract, choiceArg) =>
          EChoiceController(apply(typeId), choiceName, apply(contract), apply(choiceArg))
        case EChoiceObserver(typeId, choiceName, contract, choiceArg) =>
          EChoiceObserver(apply(typeId), choiceName, apply(contract), apply(choiceArg))
      }

  def apply(x: TypeConApp): TypeConApp = x match {
    case TypeConApp(tycon, args) => TypeConApp(apply(tycon), args.map(apply))
  }

  def apply(x: CaseAlt): CaseAlt = x match {
    case CaseAlt(pat, exp) =>
      CaseAlt(apply(pat), apply(exp))
  }

  def apply(x: CasePat): CasePat = x match {
    case CPNil | CPNone | CPDefault | CPBuiltinCon(_) | CPSome(_) | CPCons(_, _) => x
    case CPVariant(tycon, variant, binder) =>
      CPVariant(apply(tycon), variant, binder)
    case CPEnum(tycon, constructor) =>
      CPEnum(apply(tycon), constructor)
  }

  private def apply(x: Update): Update =
    x match {
      case UpdatePure(typ, expr) =>
        UpdatePure(apply(typ), apply(expr))
      case UpdateBlock(bindings, body) =>
        UpdateBlock(bindings.map(apply), apply(body))
      case UpdateCreate(templateId, arg) =>
        UpdateCreate(apply(templateId), apply(arg))
      case UpdateCreateInterface(interface, arg) =>
        UpdateCreateInterface(apply(interface), apply(arg))
      case UpdateFetchTemplate(templateId, contractId) =>
        UpdateFetchTemplate(apply(templateId), apply(contractId))
      case UpdateSoftFetchTemplate(templateId, contractId) =>
        UpdateSoftFetchTemplate(apply(templateId), apply(contractId))
      case UpdateFetchInterface(interface, contractId) =>
        UpdateFetchInterface(apply(interface), apply(contractId))
      case UpdateExercise(templateId, choice, cid, arg) =>
        UpdateExercise(apply(templateId), choice, apply(cid), apply(arg))
      case UpdateSoftExercise(templateId, choice, cid, arg) =>
        UpdateSoftExercise(apply(templateId), choice, apply(cid), apply(arg))
      case UpdateDynamicExercise(templateId, choice, cid, arg) =>
        UpdateDynamicExercise(apply(templateId), choice, apply(cid), apply(arg))
      case UpdateExerciseInterface(interface, choice, cid, arg, guard) =>
        UpdateExerciseInterface(
          apply(interface),
          choice,
          apply(cid),
          apply(arg),
          guard.map(apply(_)),
        )
      case UpdateExerciseByKey(templateId, choice, key, arg) =>
        UpdateExerciseByKey(apply(templateId), choice, apply(key), apply(arg))
      case UpdateGetTime => x
      case UpdateFetchByKey(rbk) =>
        UpdateFetchByKey(apply(rbk))
      case UpdateLookupByKey(rbk) =>
        UpdateLookupByKey(apply(rbk))
      case UpdateEmbedExpr(typ, body) =>
        UpdateEmbedExpr(apply(typ), apply(body))
      case UpdateTryCatch(typ, body, binder, handler) =>
        UpdateTryCatch(apply(typ), apply(body), binder, apply(handler))
    }

  def apply(x: RetrieveByKey): RetrieveByKey = x match {
    case RetrieveByKey(templateId, key) =>
      RetrieveByKey(apply(templateId), apply(key))
  }

  def apply(binding: Binding): Binding = binding match {
    case Binding(binder, typ, bound) =>
      Binding(binder, apply(typ), apply(bound))
  }

  def apply(x: Scenario): Scenario =
    x match {
      case ScenarioPure(typ, expr) =>
        ScenarioPure(apply(typ), apply(expr))
      case ScenarioBlock(bindings, body) =>
        ScenarioBlock(bindings.map(apply), apply(body))
      case ScenarioCommit(party, update, retType) =>
        ScenarioCommit(apply(party), apply(update), apply(retType))
      case ScenarioMustFailAt(party, update, retType) =>
        ScenarioMustFailAt(apply(party), apply(update), apply(retType))
      case ScenarioPass(relTime) =>
        ScenarioPass(apply(relTime))
      case ScenarioGetTime => x
      case ScenarioGetParty(name) =>
        ScenarioGetParty(apply(name))
      case ScenarioEmbedExpr(typ, body) =>
        ScenarioEmbedExpr(apply(typ), apply(body))
    }

  def apply(x: Definition): Definition =
    x match {
      case DDataType(serializable, params, DataRecord(fields)) =>
        DDataType(serializable, params, DataRecord(fields.map(apply)))
      case DDataType(serializable, params, DataVariant(variants)) =>
        DDataType(serializable, params, DataVariant(variants.map(apply)))
      case DDataType(serializable @ _, params @ _, DataEnum(values @ _)) =>
        x
      case DDataType(serializable @ _, params @ _, DataInterface) =>
        x
      case DValue(typ, body, isTest) =>
        DValue(apply(typ), apply(body), isTest)

      case DTypeSyn(params @ _, typ @ _) =>
        throw new RuntimeException("TODO #3616,AstRewriter,DTypeSyn")
    }

  def apply(x: Template): Template =
    x match {
      case Template(
            param,
            precond,
            signatories,
            choices,
            observers,
            key,
            implements,
          ) =>
        Template(
          param,
          apply(precond),
          apply(signatories),
          choices.transform { (_, x) =>
            apply(x)
          },
          apply(observers),
          key.map(apply),
          implements.map { case (t, x) => (apply(t), apply(x)) },
        )
    }

  def apply(x: TemplateChoice): TemplateChoice =
    x match {
      case TemplateChoice(
            name,
            consuming,
            controllers,
            observers,
            authorizers,
            selfBinder,
            (argBinderVar, argBinderType),
            returnType,
            update,
          ) =>
        TemplateChoice(
          name,
          consuming,
          apply(controllers),
          observers.map(apply),
          authorizers.map(apply),
          selfBinder,
          (argBinderVar, apply(argBinderType)),
          apply(returnType),
          apply(update),
        )
    }

  def apply(x: TemplateImplements): TemplateImplements =
    x match {
      case TemplateImplements(
            interface,
            body,
          ) =>
        TemplateImplements(
          apply(interface),
          apply(body),
        )
    }
  def apply(x: InterfaceInstanceBody): InterfaceInstanceBody =
    x match {
      case InterfaceInstanceBody(
            methods,
            view,
          ) =>
        InterfaceInstanceBody(
          methods.transform((_, x) => apply(x)),
          apply(view),
        )
    }
  def apply(x: InterfaceInstanceMethod): InterfaceInstanceMethod =
    x match {
      case InterfaceInstanceMethod(
            name,
            value,
          ) =>
        InterfaceInstanceMethod(
          name,
          apply(value),
        )
    }

  def apply(x: TemplateKey): TemplateKey =
    x match {
      case TemplateKey(typ, body, maintainers) =>
        TemplateKey(apply(typ), apply(body), apply(maintainers))
    }

  def apply(x: DefException): DefException =
    x match {
      case DefException(message) => DefException(apply(message))
    }

  def apply(x: InterfaceMethod): InterfaceMethod =
    x match {
      case InterfaceMethod(name, returnType) =>
        InterfaceMethod(name, apply(returnType))
    }

  def apply(x: DefInterface): DefInterface =
    x match {
      case DefInterface(requires, param, choices, methods, view) =>
        DefInterface(
          requires.map(apply(_)),
          param,
          choices.transform((_, v) => apply(v)),
          methods.transform((_, v) => apply(v)),
          apply(view),
        )
    }
}

object AstRewriter {

  private implicit final class TupleImmArrayOps[A, B](val array: ImmArray[(A, B)]) extends AnyVal {

    def transform[C](f: (A, B) => C): ImmArray[(A, C)] = array.map { case (k, v) => k -> f(k, v) }

  }
}
