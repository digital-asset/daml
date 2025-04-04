// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.dependencygraph

import com.digitalasset.daml.lf.codegen.exception.UnsupportedTypeError
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.typesig.DefInterface
import com.digitalasset.daml.lf.typesig.PackageSignature.TypeDecl

/** Represents a collection of templates and all the type
  * declarations for which code must be generated so that
  * the output of the codegen compiles while only targeting
  * template definitions that can be observed through the
  * Ledger API.
  */
final case class TransitiveClosure(
    serializableTypes: Vector[(Identifier, TypeDecl)],
    interfaces: Vector[(Identifier, DefInterface.FWT)],
    errors: List[UnsupportedTypeError],
)

object TransitiveClosure {

  def from(orderedDependencies: OrderedDependencies[Identifier, NodeType]): TransitiveClosure = {
    val (serializableTypes, interfaces) = orderedDependencies.deps.partitionMap {
      case (id, Node(NodeType.Internal(defDataType), _)) =>
        Left(id -> TypeDecl.Normal(defDataType))
      case (id, Node(NodeType.Root.Template(record, template), _)) =>
        Left(id -> TypeDecl.Template(record, template))
      case (id, Node(NodeType.Root.Interface(defInterface), _)) =>
        Right(id -> defInterface)
    }
    TransitiveClosure(serializableTypes, interfaces, orderedDependencies.errors)
  }

}
