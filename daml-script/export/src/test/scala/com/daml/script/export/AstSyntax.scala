// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.language.{Ast, StablePackagesV2}
import com.daml.lf.language.Util._

private[export] object AstSyntax {
  implicit class AstArrow(private val rhs: Ast.Type) extends AnyVal {
    def =>:(lhs: Ast.Type): Ast.Type = {
      TFun(lhs, rhs)
    }
  }
  def tuple(tys: Ast.Type*): Ast.Type = {
    val tupleName = StablePackagesV2.DA_Types.assertIdentifier("Tuple" + tys.length.toString)
    TTyConApp(tupleName, tys.to(ImmArray))
  }
  val vFoo: Ref.Name = Ref.Name.assertFromString("foo")
  val vBar: Ref.Name = Ref.Name.assertFromString("bar")
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
