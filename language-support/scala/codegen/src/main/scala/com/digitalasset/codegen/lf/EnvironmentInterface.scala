// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen
package lf

import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.{Dar, iface}
import iface.reader

import scala.collection.breakOut
import scala.collection.immutable.Map
import scalaz.syntax.std.map._
import scalaz.Semigroup

final case class EnvironmentInterface(typeDecls: Map[Identifier, reader.InterfaceType])

object EnvironmentInterface {
  def fromReaderInterfaces(i: reader.Interface, o: reader.Interface*): EnvironmentInterface =
    EnvironmentInterface((i +: o).flatMap {
      case reader.Interface(packageId, typeDecls) =>
        typeDecls mapKeys (Identifier(packageId, _))
    }(breakOut))

  def fromReaderInterfaces(dar: Dar[reader.Interface]): EnvironmentInterface =
    fromReaderInterfaces(dar.main, dar.dependencies: _*)

  val environmentInterfaceSemigroup = new Semigroup[EnvironmentInterface] {
    override def append(
        f1: EnvironmentInterface,
        f2: => EnvironmentInterface): EnvironmentInterface = {
      EnvironmentInterface(f1.typeDecls ++ f2.typeDecls)
    }
  }
}
