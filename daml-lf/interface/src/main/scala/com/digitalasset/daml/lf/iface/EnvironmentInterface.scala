// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package iface

import com.daml.lf.archive.Dar
import data.Ref.{Identifier, PackageId}

import scala.collection.immutable.Map
import scalaz.syntax.std.map._
import scalaz.Semigroup

/** The combination of multiple [[Interface]]s, such as from a dar. */
final case class EnvironmentInterface(
    metadata: Map[PackageId, PackageMetadata],
    typeDecls: Map[Identifier, InterfaceType],
    astInterfaces: Map[Identifier, DefInterface.FWT],
)

object EnvironmentInterface {
  def fromReaderInterfaces(i: Interface, o: Interface*): EnvironmentInterface = {
    val typeDecls = (i +: o).iterator.flatMap { case Interface(packageId, _, typeDecls, _) =>
      typeDecls mapKeys (Identifier(packageId, _))
    }.toMap
    val astInterfaces = (i +: o).iterator.flatMap {
      case Interface(packageId, _, _, astInterfaces) =>
        astInterfaces mapKeys (Identifier(packageId, _))
    }.toMap
    val metadata = (i +: o).iterator.flatMap { case Interface(packageId, metadata, _, _) =>
      metadata.iterator.map(md => packageId -> md)
    }.toMap
    EnvironmentInterface(metadata, typeDecls, astInterfaces)
  }

  def fromReaderInterfaces(dar: Dar[Interface]): EnvironmentInterface =
    fromReaderInterfaces(dar.main, dar.dependencies: _*)

  implicit val environmentInterfaceSemigroup: Semigroup[EnvironmentInterface] = Semigroup instance {
    (f1, f2) =>
      EnvironmentInterface(
        f1.metadata ++ f2.metadata,
        f1.typeDecls ++ f2.typeDecls,
        f1.astInterfaces ++ f2.astInterfaces,
      )
  }
}
