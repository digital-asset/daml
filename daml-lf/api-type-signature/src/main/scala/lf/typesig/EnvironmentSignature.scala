// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package typesig

import com.daml.lf.archive.Dar
import data.Ref, Ref.{Identifier, PackageId}

import scala.collection.immutable.Map
import scalaz.std.tuple._
import scalaz.syntax.functor._
import scalaz.syntax.std.map._
import scalaz.Semigroup

/** The combination of multiple [[PackageSignature]]s, such as from a dar. */
final case class EnvironmentSignature(
    metadata: Map[PackageId, PackageMetadata],
    typeDecls: Map[Identifier, PackageSignature.TypeDecl],
    interfaces: Map[Ref.TypeConName, DefInterface.FWT],
) {
  import PackageSignature.TypeDecl

  @deprecated("renamed to interfaces", since = "2.4.0")
  def astInterfaces: interfaces.type = interfaces

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
    * accumulated an `EnvironmentSignature` representing the whole environment.
    */
  def resolveChoices: EnvironmentSignature =
    copy(typeDecls = typeDecls.transform { (_, it) =>
      it match {
        case itpl: TypeDecl.Template =>
          val errOrTpl2 = itpl.template resolveChoices interfaces
          errOrTpl2.fold(_ => itpl, tpl2 => itpl.copy(template = tpl2))
        case z: TypeDecl.Normal => z
      }
    })

  def resolveRetroImplements: EnvironmentSignature = {
    import PackageSignature.findTemplate
    val (newTypeDecls, newInterfaces) = interfaces.foldLeft((typeDecls, interfaces)) {
      case ((typeDecls, interfaces), (ifTc, defIf)) =>
        defIf
          .resolveRetroImplements(ifTc, typeDecls) { case (typeDecls, tplName) =>
            findTemplate(typeDecls, tplName) map { itt => f =>
              typeDecls.updated(tplName, itt.copy(template = f(itt.template)))
            }
          }
          .map(defIf => interfaces.updated(ifTc, defIf))
    }
    copy(typeDecls = newTypeDecls, interfaces = newInterfaces)
  }

  def resolveInterfaceViewType(tcn: Ref.TypeConName): Option[DefInterface.ViewTypeFWT] =
    typeDecls get tcn flatMap (_.asInterfaceViewType)
}

object EnvironmentSignature {
  @deprecated("renamed to fromPackageSignatures", since = "2.4.0")
  def fromReaderInterfaces(i: PackageSignature, o: PackageSignature*): EnvironmentSignature =
    fromPackageSignatures(i, o: _*)

  def fromPackageSignatures(i: PackageSignature, o: PackageSignature*): EnvironmentSignature =
    fromPackageSignatures(i +: o)

  @deprecated("renamed to fromPackageSignatures", since = "2.4.0")
  def fromReaderInterfaces(dar: Dar[PackageSignature]): EnvironmentSignature =
    fromPackageSignatures(dar)

  def fromPackageSignatures(dar: Dar[PackageSignature]): EnvironmentSignature =
    fromPackageSignatures(dar.main, dar.dependencies: _*)

  @deprecated("renamed to fromPackageSignatures", since = "2.4.0")
  def fromReaderInterfaces(all: Iterable[PackageSignature]): EnvironmentSignature =
    fromPackageSignatures(all)

  def fromPackageSignatures(all: Iterable[PackageSignature]): EnvironmentSignature = {
    val typeDecls = all.iterator.flatMap { case PackageSignature(packageId, _, typeDecls, _) =>
      typeDecls mapKeys (Identifier(packageId, _))
    }.toMap
    val astInterfaces = all.iterator.flatMap {
      case PackageSignature(packageId, _, _, astInterfaces) =>
        astInterfaces mapKeys (Identifier(packageId, _))
    }.toMap
    val metadata = all.iterator.flatMap { case PackageSignature(packageId, metadata, _, _) =>
      metadata.iterator.map(md => packageId -> md)
    }.toMap
    EnvironmentSignature(metadata, typeDecls, astInterfaces)
  }

  implicit val environmentInterfaceSemigroup: Semigroup[EnvironmentSignature] = Semigroup instance {
    (f1, f2) =>
      EnvironmentSignature(
        f1.metadata ++ f2.metadata,
        f1.typeDecls ++ f2.typeDecls,
        f1.interfaces ++ f2.interfaces,
      )
  }
}
