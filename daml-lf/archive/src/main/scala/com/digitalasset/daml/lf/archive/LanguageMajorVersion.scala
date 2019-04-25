// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package archive

// an ADT version of the DAML-LF version
sealed abstract class LanguageMajorVersion(
    val maxSupportedMinorVersion: LanguageMinorVersion,
    previousMinorVersions: List[LanguageMinorVersion])
    extends LfVersions(maxSupportedMinorVersion, previousMinorVersions)(identity)
    with Product
    with Serializable {

  // do *not* use implicitly unless type `LanguageMinorVersion` becomes
  // indexed by the enclosing major version's singleton type --SC
  def minorVersionOrdering: Ordering[LanguageMinorVersion] =
    Ordering.by(acceptedVersions.zipWithIndex.toMap)

  val supportedMinorVersions: List[LanguageMinorVersion] =
    acceptedVersions

  final def supportsMinorVersion(fromLFFile: LanguageMinorVersion): Boolean =
    isAcceptedVersion(fromLFFile).isDefined
}

object LanguageMajorVersion {

  // Note that DAML-LF v0 never had and never will have minor versions.
  case object V0
      extends LanguageMajorVersion(maxSupportedMinorVersion = "", previousMinorVersions = List())

  case object V1
      extends LanguageMajorVersion(
        maxSupportedMinorVersion = "3",
        previousMinorVersions = List("0", "1", "2"))

  val All: List[LanguageMajorVersion] = List(V0, V1)

  @deprecated("use All instead", since = "100.12.11")
  val supported: List[LanguageMajorVersion] = All

  val ordering: Ordering[LanguageMajorVersion] =
    Ordering.by(All.zipWithIndex.toMap)
}
