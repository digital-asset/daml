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
    orderedDependencies: Vector[(Identifier, Either[DefInterface.FWT, InterfaceType])],
    errors: List[UnsupportedTypeError],
) {

  lazy val (interfaces, serializableTypes) = orderedDependencies.partitionMap {
    case (id, Left(interface)) => Left(id -> interface)
    case (id, Right(serializable)) => Right(id -> serializable)
  }

}
