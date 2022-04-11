// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.dependencygraph

import com.daml.lf.data.Ref.Identifier
import com.daml.lf.iface.{InterfaceType, DefInterface}
import scalaz.std.list._
import scalaz.syntax.bifoldable._
import scalaz.syntax.foldable._

private[codegen] object DependencyGraph {

  import com.daml.lf.codegen.Util.genTypeTopLevelDeclNames

  private def toNode(namedInterfaceType: (Identifier, InterfaceType)) =
    namedInterfaceType match {
      case id -> InterfaceType.Normal(t) =>
        Left(
          id -> Node(
            NodeType.Internal(t),
            t.bifoldMap(genTypeTopLevelDeclNames)(genTypeTopLevelDeclNames),
          )
        )
      case id -> InterfaceType.Template(rec, template) =>
        val recDeps = rec.foldMap(genTypeTopLevelDeclNames)
        val choiceAndKeyDeps = template.foldMap(genTypeTopLevelDeclNames)
        Right(
          id -> Node(
            NodeType.Root.Template(rec, template),
            recDeps ++ choiceAndKeyDeps,
          )
        )
    }

  private def toNode(interface: DefInterface.FWT) =
    Node(
      NodeType.Root.Interface(interface),
      interface.foldMap(genTypeTopLevelDeclNames),
    )

  def orderedDependencies(
      serializableTypes: Map[Identifier, InterfaceType],
      interfaces: Map[Identifier, DefInterface.FWT],
  ): OrderedDependencies[Identifier, NodeType] = {
    // invariant: no type decl name equals any template alias
    val (typeDeclNodes, templateNodes) = serializableTypes.view.partitionMap(toNode)
    val interfaceNodes = interfaces.view.mapValues(toNode)
    Graph.cyclicDependencies(internalNodes = typeDeclNodes, roots = templateNodes ++ interfaceNodes)
  }

  /** Computes the collection of templates in the `library` and
    * all the type declarations for which code must be generated
    * so that the output of the codegen compiles while only
    * targeting template definitions that can be observed through
    * the Ledger API.
    */
  def transitiveClosure(
      serializableTypes: Map[Identifier, InterfaceType],
      interfaces: Map[Identifier, DefInterface.FWT],
  ): TransitiveClosure =
    TransitiveClosure.from(orderedDependencies(serializableTypes, interfaces))

}
