// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.dependencygraph

import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.typesig.DefInterface
import com.digitalasset.daml.lf.typesig.PackageSignature.TypeDecl
import scalaz.std.list._
import scalaz.syntax.bifoldable._
import scalaz.syntax.foldable._

private[codegen] object DependencyGraph {

  import com.digitalasset.daml.lf.codegen.Util.genTypeTopLevelDeclNames

  private def toNode(namedTypeDecl: (Identifier, TypeDecl)) =
    namedTypeDecl match {
      case id -> TypeDecl.Normal(t) =>
        Left(
          id -> Node(
            NodeType.Internal(t),
            t.bifoldMap(genTypeTopLevelDeclNames)(genTypeTopLevelDeclNames),
          )
        )
      case id -> TypeDecl.Template(rec, template) =>
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
      interface.foldMap(genTypeTopLevelDeclNames) ++ interface.viewType.toList,
    )

  def orderedDependencies(
      serializableTypes: Map[Identifier, TypeDecl],
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
      serializableTypes: Map[Identifier, TypeDecl],
      interfaces: Map[Identifier, DefInterface.FWT],
  ): TransitiveClosure =
    TransitiveClosure.from(orderedDependencies(serializableTypes, interfaces))

}
