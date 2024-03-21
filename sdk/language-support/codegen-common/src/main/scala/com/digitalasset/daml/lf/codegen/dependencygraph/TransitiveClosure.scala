// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.dependencygraph

import com.daml.lf.codegen.exception.UnsupportedTypeError
import com.daml.lf.data.Ref.Identifier
import com.daml.lf.iface.{InterfaceType, DefInterface}

/** Represents a collection of templates and all the type
  * declarations for which code must be generated so that
  * the output of the codegen compiles while only targeting
  * template definitions that can be observed through the
  * Ledger API.
  */
final case class TransitiveClosure(
    serializableTypes: Vector[(Identifier, InterfaceType)],
    interfaces: Vector[(Identifier, DefInterface.FWT)],
    errors: List[UnsupportedTypeError],
)

object TransitiveClosure {

  def from(orderedDependencies: OrderedDependencies[Identifier, NodeType]): TransitiveClosure = {
    val (serializableTypes, interfaces) = orderedDependencies.deps.partitionMap {
      case (id, Node(NodeType.Internal(defDataType), _)) =>
        Left(id -> InterfaceType.Normal(defDataType))
      case (id, Node(NodeType.Root.Template(record, template), _)) =>
        Left(id -> InterfaceType.Template(record, template))
      case (id, Node(NodeType.Root.Interface(defInterface), _)) =>
        Right(id -> defInterface)
    }
    TransitiveClosure(serializableTypes, interfaces, orderedDependencies.errors)
  }

}
