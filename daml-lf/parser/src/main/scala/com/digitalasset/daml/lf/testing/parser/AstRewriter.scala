// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing.parser

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._

import scala.{PartialFunction => PF}

private[digitalasset] class AstRewriter(
    typeRule: PF[Type, Type] = PF.empty[Type, Type],
    exprRule: PF[Expr, Expr] = PF.empty[Expr, Expr],
    identifierRule: PF[Identifier, Identifier] = PF.empty[Identifier, Identifier]
) {

  import AstRewriter._

  def apply(pkg: Package): Package =
    Package(
      ImmArray(pkg.modules)
        .transform { (_, x) =>
          apply(x)
        }
        .toSeq
        .toMap,
      Set.empty[PackageId])

  def apply(module: Module): Module =
    module match {
      case Module(name, definitions, languageVersion, featureFlags) =>
        Module(
          name,
          ImmArray(definitions)
            .transform { (_, x) =>
              apply(x)
            }
            .toSeq
            .toMap,
          languageVersion,
          featureFlags)
    }

  def apply(identifier: Identifier): Identifier =
    if (identifierRule.isDefinedAt(identifier))
      identifierRule(identifier)
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
          TStruct(fields.map(apply))
      }

  def apply(nameWithType: (Name, Type)): (Name, Type) = nameWithType match {
    case (name, typ) => (name, apply(typ))
  }

  def apply(x: Expr): Expr =
    if (exprRule.isDefinedAt(x))
      exprRule(x)
    else
      x match {
        case EVar(_) | EBuiltin(_) | EPrimCon(_) | EPrimLit(_) | ETypeRep(_) =>
          x
        case EVal(ref) =>
          EVal(apply(ref))
        case ELocation(loc, expr) =>
          ELocation(loc, apply(expr))
        case ERecCon(tycon, fields) =>
          ERecCon(apply(tycon), fields.transform { (_, x) =>
            apply(x)
          })
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
          EToAny(ty, apply(body))
        case EFromAny(ty, body) =>
          EFromAny(ty, apply(body))
      }

  def apply(x: TypeConApp): TypeConApp = x match {
    case TypeConApp(tycon, args) => TypeConApp(apply(tycon), args.map(apply))
  }

  def apply(x: CaseAlt): CaseAlt = x match {
    case CaseAlt(pat, exp) =>
      CaseAlt(apply(pat), apply(exp))
  }

  def apply(x: CasePat): CasePat = x match {
    case CPNil | CPNone | CPDefault | CPPrimCon(_) | CPSome(_) | CPCons(_, _) => x
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
      case UpdateFetch(templateId, contractId) =>
        UpdateFetch(apply(templateId), apply(contractId))
      case UpdateExercise(templateId, choice, cid, actors, arg) =>
        UpdateExercise(apply(templateId), choice, cid, actors.map(apply), apply(arg))
      case UpdateGetTime => x
      case UpdateFetchByKey(rbk) =>
        UpdateFetchByKey(apply(rbk))
      case UpdateLookupByKey(rbk) =>
        UpdateLookupByKey(apply(rbk))
      case UpdateEmbedExpr(typ, body) =>
        UpdateEmbedExpr(apply(typ), apply(body))
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
      case DDataType(serializable, params, DataRecord(fields, template)) =>
        DDataType(serializable, params, DataRecord(fields.map(apply), template.map(apply)))
      case DDataType(serializable, params, DataVariant(variants)) =>
        DDataType(serializable, params, DataVariant(variants.map(apply)))
      case DDataType(serializable @ _, params @ _, DataEnum(values @ _)) =>
        x
      case DValue(typ, noPartyLiterals, body, isTest) =>
        DValue(apply(typ), noPartyLiterals, apply(body), isTest)

      case DTypeSyn(params @ _, typ @ _) =>
        throw new RuntimeException("TODO #3616,AstRewriter,DTypeSyn")
    }

  def apply(x: Template): Template =
    x match {
      case Template(param, precond, signatories, agreementText, choices, observers, key) =>
        Template(
          param,
          apply(precond),
          apply(signatories),
          apply(agreementText),
          choices.transform { (_, x) =>
            apply(x)
          },
          apply(observers),
          key.map(apply)
        )
    }

  def apply(x: TemplateChoice): TemplateChoice =
    x match {
      case TemplateChoice(
          name,
          consuming,
          controllers,
          selfBinder,
          (argBinderVar, argBinderType),
          returnType,
          update) =>
        TemplateChoice(
          name,
          consuming,
          apply(controllers),
          selfBinder,
          (argBinderVar, apply(argBinderType)),
          apply(returnType),
          apply(update))
    }

  def apply(x: TemplateKey): TemplateKey =
    x match {
      case TemplateKey(typ, body, maintainers) =>
        TemplateKey(apply(typ), apply(body), apply(maintainers))
    }

}

object AstRewriter {

  private implicit final class TupleImmArrayOps[A, B](val array: ImmArray[(A, B)]) extends AnyVal {

    def transform[C](f: (A, B) => C): ImmArray[(A, C)] = array.map { case (k, v) => k -> f(k, v) }

  }
}
