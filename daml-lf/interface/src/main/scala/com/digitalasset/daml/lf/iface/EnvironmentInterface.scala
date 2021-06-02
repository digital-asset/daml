// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
)

object EnvironmentInterface {
  def fromReaderInterfaces(i: Interface, o: Interface*): EnvironmentInterface =
    (i +: o).foldLeft(EnvironmentInterface(Map.empty, Map.empty)) {
      case (acc, Interface(packageId, optMetadata, typeDecls)) =>
        acc.copy(
          metadata =
            optMetadata.fold(acc.metadata)(metadata => acc.metadata.updated(packageId, metadata)),
          typeDecls = acc.typeDecls ++ typeDecls.mapKeys(Identifier(packageId, _)),
        )
    }

  def fromReaderInterfaces(dar: Dar[Interface]): EnvironmentInterface =
    fromReaderInterfaces(dar.main, dar.dependencies: _*)

  implicit val environmentInterfaceSemigroup: Semigroup[EnvironmentInterface] = Semigroup instance {
    (f1, f2) =>
      EnvironmentInterface(f1.metadata ++ f2.metadata, f1.typeDecls ++ f2.typeDecls)
  }
}
