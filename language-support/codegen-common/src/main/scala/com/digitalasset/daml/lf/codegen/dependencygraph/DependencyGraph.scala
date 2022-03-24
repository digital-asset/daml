// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.dependencygraph

import com.daml.lf.codegen.Util
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.codegen.lf.DefTemplateWithRecord
import com.daml.lf.data.Ref.Identifier
import com.daml.lf.iface.{EnvironmentInterface, InterfaceType, DefDataType}
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.bifoldable._
import scalaz.syntax.bifunctor._
import scalaz.syntax.foldable._

private[codegen] object DependencyGraph {
  def orderedDependencies(
      library: EnvironmentInterface
  ): OrderedDependencies[Identifier, Either[DefTemplateWithRecord, DefDataType.FWT]] = {
    val decls = library.typeDecls
    // invariant: no type decl name equals any template alias
    val typeDeclNodes =
      decls.to(ImmArraySeq).collect { case (qualName, InterfaceType.Normal(typeDecl)) =>
        (
          qualName,
          Node(
            Right(typeDecl),
            typeDecl.bifoldMap(Util.genTypeTopLevelDeclNames)(Util.genTypeTopLevelDeclNames),
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
            Left(DefTemplateWithRecord(typ, tpl)),
            recDeps ++ choiceAndKeyDeps,
            collectDepError = true,
          ),
        )
      }
    Graph.cyclicDependencies(internalNodes = typeDeclNodes, roots = templateNodes)
  }

  /** Computes the collection of templates in the `library` and
    * all the type declarations for which code must be generated
    * so that the output of the codegen compiles while only
    * targeting template definitions that can be observed through
    * the Ledger API.
    */
  def transitiveClosure(library: EnvironmentInterface): TransitiveClosure = {
    val dependencies = orderedDependencies(library)
    val (templateIds, typeDeclarations) =
      dependencies.deps
        .partitionMap { case (templateId, Node(content, _, _)) =>
          content.bimap((templateId, _), (templateId, _))
        }
    TransitiveClosure(
      templateIds = templateIds,
      typeDeclarations = typeDeclarations,
      errors = dependencies.errors,
    )
  }

}
