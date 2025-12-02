// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.js

import com.digitalasset.daml.lf.data.Ref.{ModuleId, QualifiedName, TypeConId}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.language.Util

private object TypeGen {
  def renderType(currentModule: ModuleId, tpe: Ast.Type): String = {
    def rec(tpe: Ast.Type): String =
      tpe match {
        case Ast.TVar(name) => name

        // TBuiltin
        case Util.TUnit => "{}"
        case Util.TBool => "boolean"
        case Util.TInt64 => "damlTypes.Int"
        case Util.TText => "string"
        case Util.TTimestamp => "damlTypes.Time"
        case Util.TParty => "damlTypes.Party"
        case Util.TDate => "damlTypes.Date"
        case Ast.TBuiltin(bt) => error(s"partially applied primitive type not serializable - $bt")

        // TApp
        case Util.TNumeric(_: Ast.TNat) => "damlTypes.Numeric"
        case Util.TList(targ) => s"${rec(targ)}[]"
        case Util.TOptional(targ) => s"damlTypes.Optional<${rec(targ)}>"
        case Util.TTextMap(targ) => s"{ [key: string]: ${rec(targ)} }"
        case Util.TGenMap(karg, varg) => s"damlTypes.Map<${rec(karg)}, ${rec(varg)}>"
        case Util.TContractId(targ) => s"damlTypes.ContractId<${rec(targ)}>"
        case Util.TUpdate(_) => error("Update not serializable")
        case Util.TTyConApp(tcon, targs) =>
          val renderTCon = renderType(currentModule, tcon)
          if (targs.isEmpty) renderTCon
          else s"$renderTCon<${targs.toSeq.map(rec).mkString(", ")}>"
        case _: Ast.TApp => error(s"type application not serializable - $tpe")

        // others
        case _: Ast.TTyCon => error("lonely type constructor")
        case _: Ast.TSynApp => error("type synonym not serializable")
        case _: Ast.TForall => error("universally quantified type not serializable")
        case _: Ast.TStruct => error("structural record not serializable")
        case _: Ast.TNat => error("standalone type level natural not serializable")
      }

    rec(tpe)
  }

  def renderSerializable(currentModule: ModuleId, tpe: Ast.Type): String = {
    def rec(tpe: Ast.Type): String =
      tpe match {
        case Ast.TVar(name) => name

        // TBuiltin
        case Util.TUnit => "damlTypes.Unit"
        case Util.TBool => "damlTypes.Bool"
        case Util.TInt64 => "damlTypes.Int"
        case Util.TText => "damlTypes.Text"
        case Util.TTimestamp => "damlTypes.Time"
        case Util.TParty => "damlTypes.Party"
        case Util.TDate => "damlTypes.Date"
        case Ast.TBuiltin(bt) => error(s"partially applied primitive type not serializable - $bt")

        // TApp
        case Util.TNumeric(Ast.TNat(n)) => s"damlTypes.Numeric($n)"
        case Util.TList(targ) => s"damlTypes.List(${rec(targ)})"
        case Util.TOptional(targ) => s"damlTypes.Optional(${rec(targ)})"
        case Util.TTextMap(targ) => s"damlTypes.TextMap(${rec(targ)})"
        case Util.TGenMap(karg, varg) => s"damlTypes.Map(${rec(karg)}, ${rec(varg)})"
        case Util.TContractId(targ) => s"damlTypes.ContractId(${rec(targ)})"
        case Util.TUpdate(_) => error("Update not serializable")
        case Util.TTyConApp(tcon, targs) =>
          val renderSer = renderSerializable(currentModule, tcon)
          if (targs.isEmpty) renderSer
          else s"$renderSer(${targs.toSeq.map(rec).mkString(", ")})"
        case _: Ast.TApp => error(s"type application not serializable - $tpe")

        // others
        case _: Ast.TTyCon => error("lonely type constructor")
        case _: Ast.TSynApp => error("type synonym not serializable")
        case _: Ast.TForall => error("universally quantified type not serializable")
        case _: Ast.TStruct => error("structural record not serializable")
        case _: Ast.TNat => error("standalone type level natural not serializable")
      }

    rec(tpe)
  }

  def renderType(currentModule: ModuleId, typeCon: TypeConId): String =
    if (currentModule.pkg != typeCon.pkg)
      s"pkg${typeCon.pkg}.${dottedName(typeCon.qualifiedName)}"
    else if (currentModule.moduleName != typeCon.qualifiedName.module) {
      val moduleName = typeCon.qualifiedName.module.segments.toSeq.mkString("_")
      s"$moduleName.${typeCon.qualifiedName.name.segments.toSeq.mkString(".")}"
    } else typeCon.qualifiedName.name.dottedName

  def renderSerializable(currentModule: ModuleId, typeCon: TypeConId): String =
    if (currentModule.pkg != typeCon.pkg)
      s"pkg${typeCon.pkg}.${dottedName(typeCon.qualifiedName)}"
    else if (currentModule.moduleName != typeCon.qualifiedName.module) {
      val moduleName = typeCon.qualifiedName.module.segments.toSeq.mkString("_")
      s"$moduleName.${typeCon.qualifiedName.name.segments.toSeq.mkString(".")}"
    } else s"exports.${typeCon.qualifiedName.name}"

  private def dottedName(qualifiedName: QualifiedName): String =
    s"${qualifiedName.module}.${qualifiedName.name}"

  private def error(msg: String): Nothing = throw new RuntimeException("IMPOSSIBLE: " + msg)
}
