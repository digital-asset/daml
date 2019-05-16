// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen.dependencygraph

import com.digitalasset.daml.lf.iface._
import com.digitalasset.daml.lf.iface.reader.InterfaceType
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.codegen.{Util, lf}
import com.digitalasset.daml.lf.data.Ref
import com.typesafe.scalalogging.Logger
import lf.DefTemplateWithRecord
import scalaz.std.list._
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

  private val logger: Logger = Logger(getClass)

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
        val recDeps = typ.foldMap(Util.genTypeTopLevelDeclNames)
        val choiceDeps = tpl.foldMap(Util.genTypeTopLevelDeclNames)
        (
          qualName,
          Node(
            TemplateWrapper(DefTemplateWithRecord(typ, tpl)),
            recDeps ++ choiceDeps,
            collectDepError = true))
    }

    def matchesRoots(qualName: Ref.Identifier): Boolean =
      util.roots.exists(_.findFirstIn(qualName.qualifiedName.qualifiedName).isDefined)
    // scala-2.13-M4: _.matches(qualName.qualifiedName.qualifiedName)

    val interesting =
      if (util.roots.isEmpty)
        templateNodes
      else {
        val res = templateNodes.filter(pair => matchesRoots(pair._1))
        logger.info(s"""Filtered template names to match regex set:
             |${util.roots.mkString(", ")}.
             |Considering ${res.size} templates.
           """.stripMargin)
        logger.debug(s"Generating code starting from roots:\n" +
          s"${res.map(_._1.qualifiedName.qualifiedName).mkString("\n")}")
        res
      }

    Graph.cyclicDependencies(internalNodes = typeDeclNodes, roots = interesting)
  }

  private[this] def symmGenTypeDependencies[B[_, _]: Bifoldable](
      gts: B[Type, Type]): List[Identifier] =
    gts.bifoldMap(Util.genTypeTopLevelDeclNames)(Util.genTypeTopLevelDeclNames)
}

object DependencyGraph {
  def apply(util: lf.LFUtil): DependencyGraph[util.Interface, util.TemplateInterface] =
    LFDependencyGraph(util)
}
