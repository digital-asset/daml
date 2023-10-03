// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

import com.daml.lf.LfVersions

// an ADT version of the Daml-LF version
sealed abstract class LanguageMajorVersion(val pretty: String, minorAscending: List[String])
    extends LfVersions((minorAscending :+ "dev").map[LanguageMinorVersion](LanguageMinorVersion))(
      _.toProtoIdentifier
    )
    with Product
    with Serializable {

  // TODO(#17366): 2.dev is currently the only 2.x version, but as soon as 2.0 is introduced this
  //   code should be cleaned up.
  val minStableVersion =
    LanguageVersion(this, LanguageMinorVersion(minorAscending.headOption.getOrElse("dev")))
  val maxStableVersion =
    LanguageVersion(this, LanguageMinorVersion(minorAscending.lastOption.getOrElse("dev")))

  final def dev: LanguageVersion = {
    LanguageVersion(this, LanguageMinorVersion("dev"))
  }

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
        minorAscending = List("6", "7", "8", "11", "12", "13", "14", "15"),
      )

  case object V2
      extends LanguageMajorVersion(
        pretty = "2",
        minorAscending = List(),
      )

  val All: List[LanguageMajorVersion] = List(V1, V2)

  implicit val Ordering: scala.Ordering[LanguageMajorVersion] =
    scala.Ordering.by(All.zipWithIndex.toMap)
}
