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

object Ref2 {
  type IdTypeConRef = FullReference[PackageRef.Id]
  type NameTypeConRef = FullReference[PackageRef.Name]

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
