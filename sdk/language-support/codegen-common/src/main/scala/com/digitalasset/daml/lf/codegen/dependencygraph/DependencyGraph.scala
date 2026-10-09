// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.dependencygraph

import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.typesig._
import com.digitalasset.daml.lf.typesig.PackageSignature.TypeDecl

private[codegen] object DependencyGraph {
  private def toNode(namedTypeDecl: (Identifier, TypeDecl)) =
    namedTypeDecl match {
      case id -> TypeDecl.Normal(t) =>
        Left(
          id -> Node(
            NodeType.Internal(t),
            getTopLevelDeclNames(t.dataType),
          )
        )
      case id -> TypeDecl.Template(rec, template) =>
        Right(
          id -> Node(
            NodeType.Root.Template(rec, template),
            getTopLevelDeclNames(rec) ++ getTopLevelDeclNames(template),
          )
        )
    }

  private def toNode(interface: DefInterface.FWT) =
    Node(
      NodeType.Root.Interface(interface),
      getTopLevelDeclNames(interface) ++ interface.viewType.toList,
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

  private def getTopLevelDeclNames(dataType: DataType[Type, Type]): List[Identifier] =
    dataType match {
      case _: Enum => List.empty
      case record: Record[Type] =>
        record.fields.toList.flatMap { case (_, t) => getTopLevelDeclNames(t) }
      case variant: Variant[Type] =>
        variant.fields.toList.flatMap { case (_, t) => getTopLevelDeclNames(t) }
    }

  private def getTopLevelDeclNames(template: DefTemplate[Type]): List[Identifier] = {
    val fromChoices = template.tChoices.resolvedChoices.values.toList
      .flatMap(_.values)
      .flatMap(getTopLevelDeclNames)
    val fromKey = template.key.toList.flatMap(getTopLevelDeclNames)
    fromChoices ++ fromKey
  }

  private def getTopLevelDeclNames(interface: DefInterface[Type]): List[Identifier] =
    interface.choices.values.toList.flatMap(getTopLevelDeclNames)

  private def getTopLevelDeclNames(choice: TemplateChoice[Type]): List[Identifier] =
    getTopLevelDeclNames(choice.param) ++ getTopLevelDeclNames(choice.returnType)

  private def getTopLevelDeclNames(tpe: Type): List[Identifier] =
    tpe match {
      case TypeCon(TypeConId(nm), typArgs) => nm :: typArgs.toList.flatMap(getTopLevelDeclNames)
      case TypePrim(_, typArgs) => typArgs.toList.flatMap(getTopLevelDeclNames)
      case TypeVar(_) | TypeNumeric(_) => Nil
    }

};
