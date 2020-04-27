// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package iface

import com.daml.lf.archive.Dar
import data.Ref.Identifier

import scala.collection.breakOut
import scala.collection.immutable.Map
import scalaz.syntax.std.map._
import scalaz.Semigroup

/** The combination of multiple [[Interface]]s, such as from a dar. */
final case class EnvironmentInterface(typeDecls: Map[Identifier, InterfaceType])

object EnvironmentInterface {
  def fromReaderInterfaces(i: Interface, o: Interface*): EnvironmentInterface =
    EnvironmentInterface((i +: o).flatMap {
      case Interface(packageId, typeDecls) =>
        typeDecls mapKeys (Identifier(packageId, _))
    }(breakOut))

  def fromReaderInterfaces(dar: Dar[Interface]): EnvironmentInterface =
    fromReaderInterfaces(dar.main, dar.dependencies: _*)

  implicit val environmentInterfaceSemigroup: Semigroup[EnvironmentInterface] = Semigroup instance {
    (f1, f2) =>
      EnvironmentInterface(f1.typeDecls ++ f2.typeDecls)
  }
}
