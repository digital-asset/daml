// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.language

import com.digitalasset.daml.lf.LfVersions

import scalaz.NonEmptyList

// an ADT version of the DAML-LF version
sealed abstract class LanguageMajorVersion(
    val pretty: String,
    stableAscending: NonEmptyList[String])
    extends LfVersions(
      stableAscending.map[LanguageMinorVersion](LanguageMinorVersion.Stable) append NonEmptyList(
        LanguageMinorVersion.Dev))(_.toProtoIdentifier)
    with Product
    with Serializable {

  val maxSupportedStableMinorVersion: LanguageMinorVersion.Stable =
    LanguageMinorVersion.Stable(stableAscending.last)

  // do *not* use implicitly unless type `LanguageMinorVersion` becomes
  // indexed by the enclosing major version's singleton type --SC
  final def minorVersionOrdering: Ordering[LanguageMinorVersion] =
    Ordering.by(acceptedVersions.zipWithIndex.toMap)

  val supportedMinorVersions: List[LanguageMinorVersion] =
    acceptedVersions

  final def supportsMinorVersion(fromLFFile: String): Boolean =
    isAcceptedVersion(fromLFFile).isDefined
}

object LanguageMajorVersion {

  // Note that DAML-LF v0 never had and never will have minor versions.
  case object V0 extends LanguageMajorVersion(pretty = "0", stableAscending = NonEmptyList(""))

  case object V1
      extends LanguageMajorVersion(
        pretty = "1",
        stableAscending = NonEmptyList("0", "1", "2", "3", "4", "5", "6"))

  val All: List[LanguageMajorVersion] = List(V0, V1)

  @deprecated("use All instead", since = "100.12.12")
  val supported: List[LanguageMajorVersion] = All

  val ordering: Ordering[LanguageMajorVersion] =
    Ordering.by(All.zipWithIndex.toMap)
}
