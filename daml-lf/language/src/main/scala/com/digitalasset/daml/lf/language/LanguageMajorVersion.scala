// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

import com.daml.lf.LfVersions

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

  final val maxSupportedStableMinorVersion: LanguageMinorVersion.Stable =
    LanguageMinorVersion.Stable(stableAscending.last)

  // do *not* use implicitly unless type `LanguageMinorVersion` becomes
  // indexed by the enclosing major version's singleton type --SC
  final val minorVersionOrdering: Ordering[LanguageMinorVersion] =
    Ordering.by(acceptedVersions.zipWithIndex.toMap)

  final val supportedMinorVersions: List[LanguageMinorVersion] =
    acceptedVersions

  final def supportsMinorVersion(fromLFFile: String): Boolean =
    isAcceptedVersion(fromLFFile).isDefined
}

object LanguageMajorVersion {

  case object V1
      extends LanguageMajorVersion(pretty = "1", stableAscending = NonEmptyList("6", "7", "8"))

  val All: List[LanguageMajorVersion] = List(V1)

  @deprecated("use All instead", since = "100.12.12")
  val supported: List[LanguageMajorVersion] = All

  val ordering: Ordering[LanguageMajorVersion] =
    Ordering.by(All.zipWithIndex.toMap)
}
