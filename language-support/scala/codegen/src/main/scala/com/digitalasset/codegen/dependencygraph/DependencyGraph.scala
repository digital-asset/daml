// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen.dependencygraph

import com.digitalasset.daml.lf.iface._
import com.digitalasset.daml.lf.iface.reader.InterfaceType
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.codegen.{Util, lf}
import lf.{DefTemplateWithRecord, EnvironmentInterface}

import scalaz.std.list._
import scalaz.std.tuple._
import scalaz.syntax.bifoldable._
import scalaz.syntax.foldable._
import scalaz.Bifoldable

import scala.language.higherKinds

sealed abstract class DependencyGraph[Iface, TmplI] {
  def orderedDependencies(
      library: Iface): OrderedDependencies[Identifier, TypeDeclOrTemplateWrapper[TmplI]]
}

private final case class LFDependencyGraph(private val util: lf.LFUtil)
    extends DependencyGraph[lf.LFUtil#Interface, lf.LFUtil#TemplateInterface] {
  def orderedDependencies(library: EnvironmentInterface)
    : OrderedDependencies[Identifier, TypeDeclOrTemplateWrapper[DefTemplateWithRecord.FWT]] = {
    val EnvironmentInterface(decls) = library
    // invariant: no type decl name equals any template alias
    val typeDeclNodes = decls.collect {
      case (qualName, InterfaceType.Normal(typeDecl)) =>
        (
          qualName,
          Node(
            TypeDeclWrapper(typeDecl),
            symmGenTypeDependencies(typeDecl),
            collectDepError = false))
    }
    val templateNodes = decls.collect {
      case (qualName, InterfaceType.Template(typ, tpl)) =>
        val recDeps = typ.foldMap((fwt: FieldWithType) => Util.genTypeTopLevelDeclNames(fwt._2))
        val choiceDeps = tpl.foldMap(Util.genTypeTopLevelDeclNames)
        (
          qualName,
          Node(
            TemplateWrapper(DefTemplateWithRecord(typ, tpl)),
            recDeps ++ choiceDeps,
            collectDepError = true))
    }
    Graph.cyclicDependencies(internalNodes = typeDeclNodes, roots = templateNodes)
  }

  private[this] def genTypeDependencies[B[_, _]: Bifoldable, I](gts: B[I, Type]): List[Identifier] =
    Bifoldable[B].rightFoldable.foldMap(gts)(Util.genTypeTopLevelDeclNames)

  private[this] def symmGenTypeDependencies[B[_, _]: Bifoldable, I, J](
      gts: B[(I, Type), (J, Type)]): List[Identifier] =
    gts.bifoldMap(genTypeDependencies(_))(genTypeDependencies(_))
}

object DependencyGraph {
  def apply(util: lf.LFUtil): DependencyGraph[util.Interface, util.TemplateInterface] =
    LFDependencyGraph(util)
}
