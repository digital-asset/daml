// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen.dependencygraph

import com.daml.lf.iface._
import com.daml.lf.data.Ref.Identifier
import com.daml.codegen.{Util, lf}
import com.daml.lf.data.ImmArray.ImmArraySeq
import lf.DefTemplateWithRecord
import scalaz.std.list._
import scalaz.syntax.bifoldable._
import scalaz.syntax.foldable._
import scalaz.Bifoldable

import scala.collection.compat._

sealed abstract class DependencyGraph[Iface, TmplI] {
  def orderedDependencies(
      library: Iface
  ): OrderedDependencies[Identifier, TypeDeclOrTemplateWrapper[TmplI]]
}

private final case class LFDependencyGraph(private val util: lf.LFUtil)
    extends DependencyGraph[lf.LFUtil#Interface, lf.LFUtil#TemplateInterface] {
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
            symmGenTypeDependencies(typeDecl),
            collectDepError = false,
          ),
        )
      }
    val templateNodes =
      decls.to(ImmArraySeq).collect { case (qualName, InterfaceType.Template(typ, tpl)) =>
        val recDeps = typ.foldMap(Util.genTypeTopLevelDeclNames)
        val choiceAndKeyDeps = tpl.foldMap(Util.genTypeTopLevelDeclNames)
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

  private[this] def symmGenTypeDependencies[B[_, _]: Bifoldable](
      gts: B[Type, Type]
  ): List[Identifier] =
    gts.bifoldMap(Util.genTypeTopLevelDeclNames)(Util.genTypeTopLevelDeclNames)
}

object DependencyGraph {
  def apply(util: lf.LFUtil): DependencyGraph[util.Interface, util.TemplateInterface] =
    LFDependencyGraph(util)
}
