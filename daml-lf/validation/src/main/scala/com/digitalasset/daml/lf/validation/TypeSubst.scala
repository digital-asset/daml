// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.daml.lf.data.Ref.Name
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.validation.Util._
import com.digitalasset.daml.lf.validation.traversable.TypeTraversable

private[validation] case class TypeSubst(map: Map[TypeVarName, Type], private val depth: Int = 0) {

  lazy val freeVars: Set[TypeVarName] =
    (Set.empty[TypeVarName] /: map.values)(TypeSubst.freeVars)

  def apply(typ: Type): Type = typ match {
    case TVar(name) => map.getOrElse(name, typ)
    case TTyCon(_) | TBuiltin(_) => typ
    case TApp(t1, t2) => TApp(apply(t1), apply(t2))
    case TForall((v, k), t) =>
      val (v1, subst1) = if (freeVars.contains(v)) {
        val v1 = freshTypeVarName
        v1 -> TypeSubst(map + (v -> TVar(v1)))
      } else
        v -> TypeSubst(map - v)
      TForall((v1, k), subst1(t))
    case TTuple(ts) => TTuple(ts.mapValues(apply))
  }

  private def freshTypeVarName: TypeVarName =
    Stream
      .from(0)
      .map(i => Name.assertFromString("$freshVar" + i.toString))
      .filterNot(freeVars.contains)(0)

  def apply(dataCons: DataCons): DataCons = dataCons match {
    case DataRecord(fields, optTemplate: Option[Template]) =>
      DataRecord(fields.mapValues(apply), optTemplate.map(apply))
    case DataVariant(variants) =>
      DataVariant(variants.mapValues(apply))
  }

  def apply(tmpl: Template): Template = tmpl match {
    case Template(param, precond, signatories, agreementText, choices, observers, mbKey) =>
      Template(
        param,
        apply(precond),
        apply(signatories),
        apply(agreementText),
        choices.mapValues(apply),
        apply(observers),
        mbKey.map(apply))
  }

  def apply(binding: Binding): Binding = binding match {
    case Binding(binder, typ, bound) =>
      Binding(binder, apply(typ), apply(bound))
  }

  def apply(app: TypeConApp): TypeConApp = app match {
    case TypeConApp(tycon, args) => TypeConApp(tycon, args.map(apply))
  }

  def apply(alt: CaseAlt): CaseAlt = alt match {
    case CaseAlt(pattern, expr) => CaseAlt(pattern, apply(expr))
  }

  def apply(update: Update): Update = update match {
    case UpdateGetTime =>
      update
    case UpdatePure(t, expr) =>
      UpdatePure(apply(t), apply(expr))
    case UpdateBlock(bindings, body) =>
      UpdateBlock(bindings.map(apply), apply(body))
    case UpdateCreate(templateId, arg) =>
      UpdateCreate(templateId, apply(arg))
    case UpdateFetch(templateId, contractId) =>
      UpdateFetch(templateId, apply(contractId))
    case UpdateExercise(templateId, choice, cidE, actorsE, argE) =>
      UpdateExercise(templateId, choice, apply(cidE), apply(actorsE), apply(argE))
    case UpdateEmbedExpr(typ, body) =>
      UpdateEmbedExpr(apply(typ), apply(body))
    case UpdateLookupByKey(retrieveByKey) =>
      UpdateLookupByKey(apply(retrieveByKey))
    case UpdateFetchByKey(retrieveByKey) =>
      UpdateFetchByKey(apply(retrieveByKey))
  }

  def apply(retrieveByKey: RetrieveByKey): RetrieveByKey = {
    retrieveByKey.copy(key = apply(retrieveByKey.key))
  }

  def apply(scenario: Scenario): Scenario = scenario match {
    case ScenarioGetTime | ScenarioGetParty(_) => scenario
    case ScenarioPure(typ, expr) =>
      ScenarioPure(apply(typ), apply(expr))
    case ScenarioBlock(bindings, body) =>
      ScenarioBlock(bindings.map(apply), apply(body))
    case ScenarioCommit(party, update, typ) =>
      ScenarioCommit(apply(party), apply(update), apply(typ))
    case ScenarioMustFailAt(party, update, typ) =>
      ScenarioMustFailAt(apply(party), apply(update), apply(typ))
    case ScenarioPass(delta) =>
      ScenarioPass(apply(delta))
    case ScenarioEmbedExpr(typ, exp) =>
      ScenarioEmbedExpr(apply(typ), apply(exp))
  }

  def apply(expr0: Expr): Expr = expr0 match {
    case EVar(_) | EVal(_) | EBuiltin(_) | EPrimCon(_) | EPrimLit(_) | EContractId(_, _) =>
      expr0
    case ERecCon(tycon, fields) =>
      ERecCon(apply(tycon), fields.mapValues(apply))
    case ERecProj(tycon, field, record) =>
      ERecProj(apply(tycon), field, apply(record))
    case ERecUpd(tycon, field, record, update) =>
      ERecUpd(apply(tycon), field, apply(record), apply(update))
    case EVariantCon(tycon, variant, arg) =>
      EVariantCon(apply(tycon), variant, apply(arg))
    case ETupleCon(fields) =>
      ETupleCon(fields.mapValues(apply))
    case ETupleProj(field, tuple) =>
      ETupleProj(field, apply(tuple))
    case ETupleUpd(field, tuple, update) =>
      ETupleUpd(field, apply(tuple), apply(update))
    case EApp(fun, arg) =>
      EApp(apply(fun), apply(arg))
    case ETyApp(expr, typ) =>
      ETyApp(apply(expr), apply(typ))
    case EAbs((varName, typ), body, ref) =>
      EAbs(varName -> apply(typ), apply(body), ref)
    case ETyAbs((vName, kind), body) =>
      ETyAbs(vName -> kind, apply(body))
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
    case ELocation(loc, expr) =>
      ELocation(loc, apply(expr))
    case ENone(typ) =>
      ENone(apply(typ))
    case ESome(typ, body) =>
      ESome(apply(typ), apply(body))
  }

  def apply(choice: TemplateChoice): TemplateChoice = choice match {
    case TemplateChoice(
        name,
        consuming,
        controllers,
        selfBinder,
        (varName, typ),
        returnType,
        update) =>
      TemplateChoice(
        name,
        consuming,
        apply(controllers),
        selfBinder,
        (varName, apply(typ)),
        apply(returnType),
        apply(update))
  }

  def apply(key: TemplateKey): TemplateKey = key.copy(typ = apply(key.typ))

}

object TypeSubst {

  def apply(map: (TypeVarName, Type)*): TypeSubst = new TypeSubst(map.toMap)

  def freeVars(typ: Type): Set[TypeVarName] = freeVars(Set.empty, typ)

  private def freeVars(acc: Set[TypeVarName], typ: Type): Set[TypeVarName] = typ match {
    case TVar(name) =>
      acc + name
    case otherwise @ _ =>
      (acc /: TypeTraversable(typ))(freeVars)
  }
}
