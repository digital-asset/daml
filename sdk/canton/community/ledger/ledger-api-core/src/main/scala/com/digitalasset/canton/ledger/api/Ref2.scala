// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
import com.digitalasset.daml.lf.data.Ref.{
  FullReference,
  Identifier,
  PackageId,
  PackageName,
  PackageRef,
  QualifiedName,
}
import com.digitalasset.daml.lf.data.assertRight

object Ref2 {
  type IdTypeConRef = FullReference[PackageRef.Id]
  type NameTypeConRef = FullReference[PackageRef.Name]

  // TODO(#26862) remove this once the PR to move this code into daml-lf is merged
  sealed abstract class FullReferenceCompanion2[X] {

    def pkgFromString(s: String): Either[String, X]

    private def splitInTwo(s: String, splitCh: Char): Option[(String, String)] = {
      val splitIndex = s.indexOf(splitCh.toInt)
      if (splitIndex < 0) None
      else Some((s.substring(0, splitIndex), s.substring(splitIndex + 1)))
    }

    final def fromString(s: String): Either[String, FullReference[X]] =
      splitInTwo(s, ':') match {
        case Some((packageString, qualifiedNameString)) =>
          for {
            pkgRef <- pkgFromString(packageString)
            qualifiedName <- QualifiedName.fromString(qualifiedNameString)
          } yield FullReference(pkgRef, qualifiedName)
        case None =>
          Left(s"Separator ':' between package identifier and qualified name not found in $s")
      }

    @throws[IllegalArgumentException]
    final def assertFromString(s: String): FullReference[X] = assertRight(fromString(s))

    final def apply(pkg: X, qualifiedName: QualifiedName): FullReference[X] =
      FullReference(pkg, qualifiedName)

    final def unapply(f: FullReference[X]): Some[(X, QualifiedName)] = Some(
      (f.pkg, f.qualifiedName)
    )
  }

  object NameTypeConRef extends FullReferenceCompanion2[PackageRef.Name] {
    override def pkgFromString(s: String): Either[String, PackageRef.Name] =
      PackageRef.fromString(s).flatMap {
        case name: PackageRef.Name => Right(name)
        case _: PackageRef.Id => Left("Expected PackageRef.Name, but got PackageRef.Id")
      }
  }

  final case class FullIdentifier(
      pkgId: PackageId,
      pkgName: PackageName,
      qualifiedName: QualifiedName,
  ) {
    def toIdentifier: Identifier = FullReference(pkgId, qualifiedName)
    def toIdTypeConRef: IdTypeConRef = FullReference(PackageRef.Id(pkgId), qualifiedName)
    def toNameTypeConRef: NameTypeConRef = FullReference(PackageRef.Name(pkgName), qualifiedName)
  }

  object FullIdentifier {
    def fromIdentifier(identifier: Identifier, pkgName: PackageName): FullIdentifier =
      FullIdentifier(identifier.pkg, pkgName, identifier.qualifiedName)
    def fromIdTypeConRef(idTypeConRef: IdTypeConRef, pkgName: PackageName): FullIdentifier =
      FullIdentifier(idTypeConRef.pkg.id, pkgName, idTypeConRef.qualifiedName)
    def fromNameTypeConRef(nameTypeConRef: NameTypeConRef, pkgId: PackageId): FullIdentifier =
      FullIdentifier(pkgId, nameTypeConRef.pkg.name, nameTypeConRef.qualifiedName)
  }

  implicit class IdentifierConverter(identifier: Identifier) {
    def toFullIdentifier(pkgName: PackageName): FullIdentifier =
      FullIdentifier.fromIdentifier(identifier, pkgName)
  }
  implicit class IdTypeConRefConverter(idTypeConRef: IdTypeConRef) {
    def toFullIdentifier(pkgName: PackageName): FullIdentifier =
      FullIdentifier.fromIdTypeConRef(idTypeConRef, pkgName)
  }
  implicit class NameTypeConRefConverter(nameTypeConRef: NameTypeConRef) {
    def toFullIdentifier(pkgId: PackageId): FullIdentifier =
      FullIdentifier.fromNameTypeConRef(nameTypeConRef, pkgId)
  }

  implicit val `IdTypeConRef to LoggingValue`: ToLoggingValue[IdTypeConRef] =
    idTypeConRef => LoggingValue.OfString(idTypeConRef.toString)

  implicit val `NameTypeConRef to LoggingValue`: ToLoggingValue[NameTypeConRef] =
    nameTypeConRef => LoggingValue.OfString(nameTypeConRef.toString)

}
