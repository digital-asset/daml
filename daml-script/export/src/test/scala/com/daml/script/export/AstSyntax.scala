// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.language.Ast

private[export] object AstSyntax {
  implicit class AstApp(private val lhs: Ast.Type) extends AnyVal {
    def :@(rhs: Ast.Type): Ast.Type = {
      Ast.TApp(lhs, rhs)
    }
  }
  implicit class AstArrow(private val rhs: Ast.Type) extends AnyVal {
    def =>:(lhs: Ast.Type): Ast.Type = {
      val arrowTy: Ast.Type = Ast.TBuiltin(Ast.BTArrow)
      Ast.TApp(Ast.TApp(arrowTy, lhs), rhs)
    }
  }
  def synApp(syn: Ref.TypeSynName, args: Ast.Type*): Ast.Type = {
    Ast.TSynApp(syn, ImmArray(args))
  }
  def tuple(tys: Ast.Type*): Ast.Type = {
    val tupleTyCon: Ast.Type = Ast.TTyCon(
      Ref.TypeConName.assertFromString(
        "40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7:DA.Types:Tuple"
      )
    )
    tys.foldLeft(tupleTyCon) { case (acc, ty) => acc :@ ty }
  }
  def list(ty: Ast.Type): Ast.Type = {
    Ast.TBuiltin(Ast.BTList) :@ ty
  }
  val unit: Ast.Type = Ast.TBuiltin(Ast.BTUnit)
  val int: Ast.Type = Ast.TBuiltin(Ast.BTInt64)
  val text: Ast.Type = Ast.TBuiltin(Ast.BTText)
  val party: Ast.Type = Ast.TBuiltin(Ast.BTParty)
  val contractId: Ast.Type = Ast.TBuiltin(Ast.BTContractId)
  val vFoo: Ast.Type = Ast.TVar(Ref.Name.assertFromString("foo"))
  val vBar: Ast.Type = Ast.TVar(Ref.Name.assertFromString("bar"))
  val tArchive: Ast.Type = Ast.TTyCon(
    Ref.TypeConName.assertFromString(
      "d14e08374fc7197d6a0de468c968ae8ba3aadbf9315476fd39071831f5923662:DA.Internal.Template:Archive"
    )
  )
  val nFoo: Ref.Identifier = Ref.TypeSynName.assertFromString(
    "e7b2c7155f6dd6fc569c2325be821f1269186a540d0408b9a0c9e30406f6b64b:Module:Foo"
  )
  val nBar: Ref.Identifier = Ref.TypeSynName.assertFromString(
    "e7b2c7155f6dd6fc569c2325be821f1269186a540d0408b9a0c9e30406f6b64b:Module:Bar"
  )
}
