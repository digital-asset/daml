// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.dependencygraph

import com.daml.lf.data.Ref.Identifier
import com.daml.lf.iface.InterfaceType
import scalaz.std.list._
import scalaz.syntax.bifoldable._
import scalaz.syntax.foldable._

private[codegen] object DependencyGraph {

  private def toNode(namedInterfaceType: (Identifier, InterfaceType)) = {
    import com.daml.lf.codegen.Util.genTypeTopLevelDeclNames
    namedInterfaceType match {
      case id -> (normal: InterfaceType.Normal) =>
        Left(
          id -> Node(
            normal,
            normal.`type`.bifoldMap(genTypeTopLevelDeclNames)(genTypeTopLevelDeclNames),
          )
        )
      case id -> (template: InterfaceType.Template) =>
        val recDeps = template.rec.foldMap(genTypeTopLevelDeclNames)
        val choiceAndKeyDeps = template.template.foldMap(genTypeTopLevelDeclNames)
        Right(
          id -> Node(
            template,
            recDeps ++ choiceAndKeyDeps,
          )
        )
    }
  }

  def orderedDependencies(
      decls: Map[Identifier, InterfaceType]
  ): OrderedDependencies[Identifier, InterfaceType] = {
    // invariant: no type decl name equals any template alias
    val (typeDeclNodes, templateNodes) = decls.view.partitionMap(toNode)
    Graph.cyclicDependencies(internalNodes = typeDeclNodes, roots = templateNodes)
  }

  /** Computes the collection of templates in the `library` and
    * all the type declarations for which code must be generated
    * so that the output of the codegen compiles while only
    * targeting template definitions that can be observed through
    * the Ledger API.
    */
  def transitiveClosure(decls: Map[Identifier, InterfaceType]): TransitiveClosure = {
    val dependencies = orderedDependencies(decls)
    TransitiveClosure(
      interfaceTypes = dependencies.deps.map { case (id, Node(t, _)) => id -> t },
      errors = dependencies.errors,
    )
  }

}
