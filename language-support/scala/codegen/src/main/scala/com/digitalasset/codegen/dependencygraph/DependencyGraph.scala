// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen.dependencygraph

import com.daml.lf.iface._
import com.daml.lf.data.Ref.Identifier
import com.daml.codegen.lf.LFUtil
import com.daml.codegen.lf
import com.daml.lf.data.ImmArray.ImmArraySeq
import lf.DefTemplateWithRecord
import scalaz.std.list._
import scalaz.syntax.bifoldable._
import scalaz.syntax.foldable._

private[codegen] object DependencyGraph {
  def orderedDependencies(
      library: EnvironmentInterface
  ): OrderedDependencies[Identifier, TypeDeclOrTemplateWrapper[DefTemplateWithRecord.FWT]] = {
    val decls = library.typeDecls
    // invariant: no type decl name equals any template alias
    val typeDeclNodes =
      decls.to(ImmArraySeq).collect { case (qualName, InterfaceType.Normal(typeDecl)) =>
        (
          qualName,
          Node(
            TypeDeclWrapper(typeDecl),
            typeDecl.bifoldMap(LFUtil.genTypeTopLevelDeclNames)(LFUtil.genTypeTopLevelDeclNames),
            collectDepError = false,
          ),
        )
      }
    val templateNodes =
      decls.to(ImmArraySeq).collect { case (qualName, InterfaceType.Template(typ, tpl)) =>
        val recDeps = typ.foldMap(LFUtil.genTypeTopLevelDeclNames)
        val choiceAndKeyDeps = tpl.foldMap(LFUtil.genTypeTopLevelDeclNames)
        (
          qualName,
          Node(
            TemplateWrapper(DefTemplateWithRecord(typ, tpl)),
            recDeps ++ choiceAndKeyDeps,
            collectDepError = true,
          ),
        )
      }
    Graph.cyclicDependencies(internalNodes = typeDeclNodes, roots = templateNodes)
  }

}
