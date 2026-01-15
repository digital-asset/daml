// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.schema

import Ordering.Implicits.*

/** Tuple of Package ID, Module Name and Entity Name uniquely identify a template or a choice.
  *
  * Tuple of Package Name, Module Name and Entity Name identify "equal in essence" entities, i.e.
  * templates or choices that could be the results of upgrades of the same ancestor and therefore
  * backward/forward compatible with each other This tuple will be used in equality checks of Maps,
  * etc.
  *
  * Package Version is a string that can be compared using SemVer rules
  */
final case class Identifier(
    packageId: PackageId,
    packageName: PackageName,
    packageVersion: PackageVersion,
    moduleName: ModuleName,
    entityName: EntityName,
):
  def uniqueName: String = s"$packageId:$moduleName:$entityName"
  def universalName: String = s"#$packageName:$moduleName:$entityName"
  def qualifiedName: String = s"$moduleName:$entityName"
  def packageNameAsPackageId: String = s"#$packageName"

  def withEntityName(entityName: String): Identifier = copy(entityName = EntityName(entityName))
  def withSuffixEntityName(suffix: String): Identifier =
    copy(entityName = EntityName(s"$entityName.$suffix"))
end Identifier

object Identifier:
  private[transcode] def fromString(str: String): Identifier = str.split(':') match
    case Array(pkgId, module, name) =>
      Identifier(
        PackageId(pkgId),
        PackageName(pkgId),
        PackageVersion.Unknown,
        ModuleName(module),
        EntityName(name),
      )
    case other =>
      throw Exception(s"Unsupported identifier format: $str")

  given Ordering[Identifier] =
    Ordering.by(x =>
      (
        x.packageName,
        x.moduleName,
        x.entityName,
        x.packageVersion.split('.').toSeq.map(_.toIntOption),
      )
    )
end Identifier
