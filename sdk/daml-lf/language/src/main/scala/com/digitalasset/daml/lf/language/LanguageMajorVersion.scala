// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

import com.daml.lf.LfVersions
import com.daml.lf.language.LanguageDevConfig.{EvaluationOrder, LeftToRight, RightToLeft}
import scalaz.{IList, NonEmptyList}

import scala.math.Ordering.Implicits.infixOrderingOps

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

  import LanguageMajorVersion._

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

  // TODO(#17366): Ideally this would be specified as a feature, but at the major version level.
  //    We may want to allow expressing this once we rework feature specifications (see the TODO
  //    on features).
  final def evaluationOrder: EvaluationOrder =
    if (this >= V2) RightToLeft else LeftToRight
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
        minorAscending = List("1"),
      )

  val All: List[LanguageMajorVersion] = List(V1, V2)

  implicit val languageMajorVersionOrdering: scala.Ordering[LanguageMajorVersion] =
    scala.Ordering.by(All.zipWithIndex.toMap)

  def fromString(str: String): Option[LanguageMajorVersion] = str match {
    case "1" => Some(V1)
    case "2" => Some(V2)
    case _ => None
  }
}
