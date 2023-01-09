// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

import com.daml.lf.LfVersions

import scalaz.NonEmptyList

// an ADT version of the Daml-LF version
sealed abstract class LanguageMajorVersion(val pretty: String, minorAscending: NonEmptyList[String])
    extends LfVersions(minorAscending.map[LanguageMinorVersion](LanguageMinorVersion))(
      _.toProtoIdentifier
    )
    with Product
    with Serializable {

  // do *not* use implicitly unless type `LanguageMinorVersion` becomes
  // indexed by the enclosing major version's singleton type --SC
  final val minorVersionOrdering: Ordering[LanguageMinorVersion] =
    Ordering.by(acceptedVersions.zipWithIndex.toMap)

  final val supportedMinorVersions: List[LanguageMinorVersion] =
    acceptedVersions

  final def supportsMinorVersion(fromLFFile: String): Boolean =
    isAcceptedVersion(fromLFFile).isDefined

  final def toVersion(minorVersion: String) =
    if (supportsMinorVersion(minorVersion)) {
      Right(LanguageVersion(this, LanguageMinorVersion(minorVersion)))
    } else {
      val supportedVersions = acceptedVersions.map(v => s"$this.${v.identifier}")
      Left(s"LF $this.$minorVersion unsupported. Supported LF versions are ${supportedVersions
          .mkString(",")}")
    }
}

object LanguageMajorVersion {

  case object V1
      extends LanguageMajorVersion(
        pretty = "1",
        minorAscending = NonEmptyList("6", "7", "8", "11", "12", "13", "14", "15", "dev"),
      )

  val All: List[LanguageMajorVersion] = List(V1)

  implicit val Ordering: scala.Ordering[LanguageMajorVersion] =
    scala.Ordering.by(All.zipWithIndex.toMap)
}
