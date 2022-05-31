// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package iface

import com.daml.lf.archive.Dar
import data.Ref, Ref.{Identifier, PackageId}

import scala.collection.immutable.Map
import scalaz.syntax.std.map._
import scalaz.Semigroup

/** The combination of multiple [[Interface]]s, such as from a dar. */
final case class EnvironmentInterface(
    metadata: Map[PackageId, PackageMetadata],
    typeDecls: Map[Identifier, InterfaceType],
    astInterfaces: Map[Ref.TypeConName, DefInterface.FWT],
) {

  /** Replace all resolvable `inheritedChoices` in `typeDecls` with concrete
    * choices copied from `astInterfaces`.  If a template has any missing choices,
    * none of its inherited choices are resolved.  Idempotent.
    *
    * This is not distributive because we delay resolution, because successful
    * lookup can require the presence of another DAR.  In other words,
    *
    * {{{
    *  l.resolveChoices |+| r.resolveChoices
    *  // is possibly less well-resolved than
    *  (l |+| r).resolveChoices
    * }}}
    *
    * Therefore there is no reason to bother with `resolveChoices` until you've
    * accumulated an `EnvironmentInterface` representing the whole environment.
    */
  def resolveChoices: EnvironmentInterface =
    copy(typeDecls = typeDecls.transform { (_, it) =>
      it match {
        case itpl: InterfaceType.Template =>
          val errOrTpl2 = itpl.template resolveChoices astInterfaces
          errOrTpl2.fold(_ => itpl, tpl2 => itpl.copy(template = tpl2))
        case z: InterfaceType.Normal => z
      }
    })
}

object EnvironmentInterface {
  def fromReaderInterfaces(i: Interface, o: Interface*): EnvironmentInterface =
    fromReaderInterfaces(i +: o)

  def fromReaderInterfaces(dar: Dar[Interface]): EnvironmentInterface =
    fromReaderInterfaces(dar.main, dar.dependencies: _*)

  def fromReaderInterfaces(all: Iterable[Interface]): EnvironmentInterface = {
    val typeDecls = all.iterator.flatMap { case Interface(packageId, _, typeDecls, _) =>
      typeDecls mapKeys (Identifier(packageId, _))
    }.toMap
    val astInterfaces = all.iterator.flatMap { case Interface(packageId, _, _, astInterfaces) =>
      astInterfaces mapKeys (Identifier(packageId, _))
    }.toMap
    val metadata = all.iterator.flatMap { case Interface(packageId, metadata, _, _) =>
      metadata.iterator.map(md => packageId -> md)
    }.toMap
    EnvironmentInterface(metadata, typeDecls, astInterfaces)
  }

  implicit val environmentInterfaceSemigroup: Semigroup[EnvironmentInterface] = Semigroup instance {
    (f1, f2) =>
      EnvironmentInterface(
        f1.metadata ++ f2.metadata,
        f1.typeDecls ++ f2.typeDecls,
        f1.astInterfaces ++ f2.astInterfaces,
      )
  }
}
