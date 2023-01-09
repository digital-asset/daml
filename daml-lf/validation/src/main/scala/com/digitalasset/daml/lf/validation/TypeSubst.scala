// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.Ref.Name
import com.daml.lf.language.Ast._
import com.daml.lf.validation.Util._
import com.daml.lf.validation.iterable.TypeIterable

private[validation] object TypeSubst {

  def substitute(subst: Map[TypeVarName, Type], typ: Type): Type =
    go(freeVars(subst), subst, typ)

  private def go(fv0: Set[TypeVarName], subst0: Map[TypeVarName, Type], typ0: Type): Type =
    typ0 match {
      case TSynApp(syn, args) => TSynApp(syn, args.map(go(fv0, subst0, _)))
      case TVar(name) => subst0.getOrElse(name, typ0)
      case TTyCon(_) | TBuiltin(_) | TNat(_) => typ0
      case TApp(t1, t2) => TApp(go(fv0, subst0, t1), go(fv0, subst0, t2))
      case TForall((v0, k), t) =>
        if (fv0.contains(v0)) {
          val v1 = freshTypeVarName(fv0)
          val fv1 = fv0 + v1
          val subst1 = subst0 + (v0 -> TVar(v1))
          TForall(v1 -> k, go(fv1, subst1, t))
        } else
          TForall(v0 -> k, go(fv0 + v0, subst0 - v0, t))
      case TStruct(ts) =>
        TStruct(ts.mapValues(go(fv0, subst0, _)))
    }

  private def freshTypeVarName(fv: Set[TypeVarName]): TypeVarName =
    LazyList
      .from(0)
      .map(i => Name.assertFromString("$freshVar" + i.toString))
      .filterNot(fv.contains)(0)

  def substitute(subst: Map[TypeVarName, Type], dataCons: DataCons): DataCons = dataCons match {
    case DataRecord(fields) =>
      DataRecord(fields.transform((_, x) => substitute(subst, x)))
    case DataVariant(variants) =>
      DataVariant(variants.transform((_, x) => substitute(subst, x)))
    case _: DataEnum | DataInterface =>
      dataCons
  }

  def substitute(subst: Map[TypeVarName, Type], app: TypeConApp): TypeConApp = app match {
    case TypeConApp(tycon, args) =>
      val fv = freeVars(subst)
      TypeConApp(tycon, args.map(go(fv, subst, _)))
  }

  def freeVars(typ: Type): Set[TypeVarName] = freeVars(Set.empty, typ)

  private def freeVars(acc: Set[TypeVarName], typ: Type): Set[TypeVarName] = typ match {
    case TVar(name) =>
      acc + name
    case otherwise @ _ =>
      (TypeIterable(typ) foldLeft acc)(freeVars)
  }

  private def freeVars(subst: Map[TypeVarName, Type]): Set[TypeVarName] =
    subst.values.foldLeft(Set.empty[TypeVarName])(freeVars)

}
