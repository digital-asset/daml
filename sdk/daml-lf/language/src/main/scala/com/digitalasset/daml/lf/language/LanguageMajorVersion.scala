// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.language

import com.digitalasset.daml.lf.LfVersions
import com.digitalasset.daml.lf.language.LanguageVersion.{LegacyMajor, Major, Minor}
import com.digitalasset.daml.lf.language.LanguageVersion.Minor.{Dev, Stable}
import scalaz.{IList, NonEmptyList}

// an ADT version of the Daml-LF version
sealed abstract class LanguageMajorVersion(val pretty: String, minorAscending: List[String])
    extends LfVersions(
      (IList.fromList(minorAscending) <::: NonEmptyList("dev"))
        .map[LanguageMinorVersion](LanguageMinorVersion)
    )(
      _.toProtoIdentifier
    )
    with Product
    with Serializable {

  def toMajor : LegacyMajor =
    pretty match {
      case "1" => Major.V1
      case "2" => Major.V2
      case _ => throw new RuntimeException("idk")
    }

  def parseMinorVersionOrThrow(input: String): Minor = {
    input match {
      // "dev" case
      case "dev" => Dev

      // All stable int cases
      case "1"   => Stable(1)
      case "2"   => Stable(2)
      case "6"   => Stable(6)
      case "7"   => Stable(7)
      case "8"   => Stable(8)
      case "11"  => Stable(11)
      case "12"  => Stable(12)
      case "13"  => Stable(13)
      case "14"  => Stable(14)
      case "15"  => Stable(15)
      case "17"  => Stable(17)

      // All other cases throw an exception
      case _     =>
        throw new IllegalArgumentException(s"Invalid language version string: '$input'")
    }
  }

  val minStableVersion =
    LanguageVersion(toMajor, parseMinorVersionOrThrow(minorAscending.head))
  // second last is stable
  val maxStableVersion =
    LanguageVersion(toMajor, parseMinorVersionOrThrow(minorAscending.last))

  final def dev: LanguageVersion = {
    LanguageVersion(toMajor, parseMinorVersionOrThrow("dev"))
  }

  // do *not* use implicitly unless type `LanguageMinorVersion` becomes
  // indexed by the enclosing major version's singleton type --SC
  final val minorVersionOrdering: Ordering[LanguageMinorVersion] =
    Ordering.by(acceptedVersions.zipWithIndex.toMap)

  final val supportedMinorVersions: List[LanguageMinorVersion] =
    acceptedVersions
}

object LanguageMajorVersion {

  case object V1
      extends LanguageMajorVersion(
        pretty = "1",
        minorAscending = List("6", "7", "8", "11", "12", "13", "14", "15", "17"),
      )

  case object V2
      extends LanguageMajorVersion(
        pretty = "2",
        minorAscending = List("1", "2"),
      )

  val All: List[LanguageMajorVersion] = List(V2)

  implicit val languageMajorVersionOrdering: scala.Ordering[LanguageMajorVersion] =
    scala.Ordering.by(All.zipWithIndex.toMap)

  def fromString(str: String): Option[LanguageMajorVersion] = str match {
    case "2" => Some(V2)
    case _ => None
  }
}


