// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package language

import Ast._
import com.daml.lf.data.Ref
import com.daml.lf.language.Util.{ENatSingleton, TFun}

object FixNat {

  def processPkg(p: Package): Package =
    p.copy(p.modules.transform((_, m) => processModule(m)))

  def processPkgs(pkgs: Map[Ref.PackageId, Package]): Map[Ref.PackageId, Package] =
    pkgs.transform((_, p) => processPkg(p))

  def processCons(cons: DataCons): DataCons =
    cons match {
      case DataRecord(fields) => DataRecord(fields.map { case (n, t) => n -> processType(t) })
      case DataVariant(variants) => DataVariant(variants.map { case (n, t) => n -> processType(t) })
      case otherwise @ (DataEnum(_) | Ast.DataInterface) => otherwise
    }

  private[this] val emptyEnv = Env()

  def processExpr(body: Expr): Expr = emptyEnv.processExpr(body)

  def processDefinition(definition: Definition): Definition =
    definition match {
      case DTypeSyn(params, typ) => DTypeSyn(params, processType(typ))
      case DDataType(serializable, params, cons) =>
        DDataType(serializable, params, processCons(cons))
      case GenDValue(typ, noPartyLiterals, body, isTest) =>
        GenDValue(processType(typ), noPartyLiterals, processExpr(body), isTest)
    }

  def processChoice(c: TemplateChoice): TemplateChoice =
    c match {
      case TemplateChoice(
            name,
            consuming,
            controllers,
            choiceObservers,
            selfBinder,
            (argBinder, argType),
            returnType,
            update,
          ) =>
        GenTemplateChoice(
          name,
          consuming,
          processExpr(controllers),
          choiceObservers.map(processExpr),
          selfBinder,
          (argBinder, processType(argType)),
          processType(returnType),
          processExpr(update),
        )
    }

  def processKey(key: TemplateKey): TemplateKey = key match {
    case TemplateKey(typ, body, maintainers) =>
      TemplateKey(processType(typ), processExpr(body), processExpr(maintainers))
  }

  def processMethod(v: TemplateImplementsMethod): TemplateImplementsMethod =
    v match {
      case TemplateImplementsMethod(name, value) =>
        TemplateImplementsMethod(name, processExpr(value))
    }

  def processImplement(i: TemplateImplements): TemplateImplements =
    i match {
      case TemplateImplements(interface, methods, inheritedChoices, precond) =>
        TemplateImplements(
          interface,
          methods.transform((_, v) => processMethod(v)),
          inheritedChoices,
          processExpr(precond),
        )
    }

  def processTemplate(template: Template): Template =
    template match {
      case Template(
            param,
            precond,
            signatories,
            agreementText,
            choices,
            observers,
            key,
            implements,
          ) =>
        Template(
          param,
          processExpr(precond),
          processExpr(signatories), // Parties agreeing to the contract.
          processExpr(agreementText), // Text the parties agree to.
          choices.transform((_, c) => processChoice(c)), // Choices available in the template.
          processExpr(observers), // Observers of the contract.
          key.map(processKey),
          implements.transform((_, i) => processImplement(i)),
        )
    }

  def processInterface(interface: DefInterface): DefInterface =
    interface match {
      case DefInterface(param, fixedChoices, methods, precond) =>
        DefInterface(
          param,
          fixedChoices.transform((_, v) => processChoice(v)),
          methods.transform { case (_, InterfaceMethod(name, typ)) =>
            InterfaceMethod(name, processType(typ))
          },
          processExpr(precond),
        )
    }

  def processException(exception: DefException): DefException =
    exception match {
      case GenDefException(message) => GenDefException(processExpr(message))
    }

  def processModule(mod: Module): Module =
    mod match {
      case Module(name, definitions, templates, exceptions, interfaces, featureFlags) =>
        Module(
          name,
          definitions.transform((_, definition) => processDefinition(definition)),
          templates.transform((_, template) => processTemplate(template)),
          exceptions.transform((_, exception) => processException(exception)),
          interfaces.transform((_, interface) => processInterface(interface)),
          featureFlags,
        )
    }

  def processType(t: Type): Type =
    t match {
      case TForall(binder @ (tvar, kind), body) =>
        if (kind == KNat)
          TForall(binder, TFun(TNatSingleton(TVar(tvar)), processType(body)))
        else
          TForall(binder, processType(body))

      ///
      case TSynApp(tysyn, args) => TSynApp(tysyn, args.map(processType))
      case TApp(tyfun, arg) => TApp(processType(tyfun), processType(arg))
      case TStruct(fields) =>
        TStruct(fields.mapValues(processType))
      case TNatSingleton(_) =>
        sys.error("unexpected TNatSingleton")
      case otherwise @ (TVar(_) | TNat(_) | TTyCon(_) | TBuiltin(_)) =>
        otherwise
    }

  def processTypeApp(t: TypeConApp) =
    t.copy(args = t.args.map(processType))

  private[this] case class Env(natTypes: Set[TypeVarName] = Set.empty) {

    def introNatType(varName: TypeVarName) =
      Env(natTypes + varName)

    def hideNatType(varName: TypeVarName) =
      Env(natTypes - varName)

    def processExpr(e: Expr): Expr =
      e match {
        case ETyApp(expr, typ) =>
          val e = ETyApp(processExpr(expr), typ)
          typ match {
            case TVar(name) if natTypes(name) =>
              EApp(e, EVar(name, true))
            case TNat(n) =>
              EApp(e, ENatSingleton(n))
            case _ =>
              e
          }
        case ETyAbs(binder @ (n, kind), body) =>
          if (kind == KNat) {
            ETyAbs(
              binder,
              EAbs(
                EVar(n, true) -> TNatSingleton(TVar(n)),
                introNatType(n).processExpr(body),
                None,
              ),
            )
          } else
            ETyAbs(binder, hideNatType(n).processExpr(body))

        ///
        case EVar(value, flag) => EVar(value, flag)
        case EVal(value) => EVal(value)
        case EBuiltin(value) => EBuiltin(value)
        case EPrimCon(value) => EPrimCon(value)
        case EPrimLit(value) => EPrimLit(value)
        case ERecCon(tycon, fields) =>
          ERecCon(processTypeApp(tycon), fields.map { case (f, v) => f -> processExpr(v) })
        case ERecProj(tycon, field, record) =>
          ERecProj(processTypeApp(tycon), field, processExpr(record))
        case ERecUpd(tycon, field, record, update) =>
          ERecUpd(processTypeApp(tycon), field, processExpr(record), processExpr(update))
        case EVariantCon(tycon, variant, arg) => EVariantCon(processTypeApp(tycon), variant, arg)
        case EEnumCon(tyConName, con) => EEnumCon(tyConName, con)
        case EStructCon(fields) => EStructCon(fields.map { case (f, v) => f -> processExpr(v) })
        case EStructProj(field, struct) => EStructProj(field, processExpr(struct))
        case EStructUpd(field, struct, update) =>
          EStructUpd(field, processExpr(struct), processExpr(update))
        case EApp(fun, arg) => EApp(processExpr(fun), processExpr(arg))

        case EAbs((v, t), body, ref) => EAbs((v, processType(t)), processExpr(body), ref)

        case ECase(scrut, alts) =>
          ECase(processExpr(scrut), alts.map(alt => alt.copy(expr = processExpr(alt.expr))))
        case ELet(Binding(binder, typ, bound), body) =>
          ELet(Binding(binder, processType(typ), processExpr(bound)), processExpr(body))
        case ENil(typ) =>
          ENil(processType(typ))
        case ECons(typ, front, tail) =>
          ECons(processType(typ), front.map(processExpr), processExpr(tail))
        case EUpdate(update) =>
          EUpdate(processUpdate(update))
        case EScenario(scenario) =>
          EScenario(processScenario(scenario))
        case ELocation(loc, expr) =>
          ELocation(loc, processExpr(expr))
        case ENone(typ) =>
          ENone(processType(typ))
        case ESome(typ, body) => ESome(processType(typ), processExpr(body))
        case EToAny(ty, body) => EToAny(processType(ty), processExpr(body))
        case EFromAny(ty, body) => EFromAny(processType(ty), processExpr(body))
        case ETypeRep(typ) => ETypeRep(processType(typ))
        case EThrow(returnType, exceptionType, exception) =>
          EThrow(processType(returnType), processType(exceptionType), processExpr(exception))
        case EToAnyException(typ, value) => EToAnyException(processType(typ), processExpr(value))
        case EFromAnyException(typ, value) =>
          EFromAnyException(processType(typ), processExpr(value))
        case EToInterface(iface, tpl, value) => EToInterface(iface, tpl, processExpr(value))
        case EFromInterface(iface, tpl, value) => EFromInterface(iface, tpl, processExpr(value))
        case ECallInterface(iface, method, value) =>
          ECallInterface(iface, method, processExpr(value))
        case EExperimental(name, typ) => EExperimental(name, processType(typ))
      }

    def processUpdate(update: Update): Update =
      update match {
        case UpdatePure(t, expr) =>
          UpdatePure(processType(t), processExpr(expr))
        case UpdateBlock(bindings, body) =>
          UpdateBlock(
            bindings.map { case Binding(binder, typ, bound) =>
              Binding(binder, processType(typ), processExpr(bound))
            },
            processExpr(body),
          )
        case UpdateCreate(templateId, arg) =>
          UpdateCreate(templateId, processExpr(arg))
        case UpdateCreateInterface(interface, arg) =>
          UpdateCreateInterface(interface, processExpr(arg))
        case UpdateFetch(templateId, contractId) =>
          UpdateFetch(templateId, processExpr(contractId))
        case UpdateFetchInterface(interface, contractId) =>
          UpdateFetchInterface(interface, processExpr(contractId))
        case UpdateExercise(templateId, choice, cidE, argE) =>
          UpdateExercise(templateId, choice, processExpr(cidE), processExpr(argE))
        case UpdateExerciseInterface(interface, choice, cidE, argE) =>
          UpdateExerciseInterface(interface, choice, processExpr(cidE), processExpr(argE))
        case UpdateExerciseByKey(templateId, choice, keyE, argE) =>
          UpdateExerciseByKey(templateId, choice, processExpr(keyE), processExpr(argE))
        case Ast.UpdateGetTime =>
          Ast.UpdateGetTime
        case UpdateFetchByKey(rbk) =>
          UpdateFetchByKey(rbk)
        case UpdateLookupByKey(rbk) =>
          UpdateLookupByKey(rbk)
        case UpdateEmbedExpr(typ, body) =>
          UpdateEmbedExpr(typ, body)
        case UpdateTryCatch(typ, body, binder, handler) =>
          UpdateTryCatch(processType(typ), processExpr(body), binder, processExpr(handler))
      }

    def processScenario(s: Scenario): Scenario =
      s match {
        case ScenarioPure(t, expr) =>
          ScenarioPure(processType(t), processExpr(expr))
        case ScenarioBlock(bindings, body) =>
          ScenarioBlock(
            bindings.map { case Binding(binder, typ, bound) =>
              Binding(binder, processType(typ), processExpr(bound))
            },
            processExpr(body),
          )
        case ScenarioCommit(partyE, updateE, retType) =>
          ScenarioCommit(processExpr(partyE), processExpr(updateE), processType(retType))
        case ScenarioMustFailAt(partyE, updateE, retType) =>
          ScenarioMustFailAt(processExpr(partyE), processExpr(updateE), processType(retType))
        case ScenarioPass(relTimeE) =>
          ScenarioPass(processExpr(relTimeE))
        case Ast.ScenarioGetTime =>
          Ast.ScenarioGetTime
        case ScenarioGetParty(nameE) =>
          ScenarioGetParty(processExpr(nameE))
        case ScenarioEmbedExpr(typ, body) =>
          ScenarioEmbedExpr(processType(typ), processExpr(body))
      }

  }
}
