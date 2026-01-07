// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package language

final case class LanguageVersion private (
    major: LanguageVersion.Major,
    minor: LanguageVersion.Minor,
) extends Ordered[LanguageVersion] {

  override def toString: String = s"${major.pretty}.${minor.pretty}"
  def pretty = toString
  def isDevVersion: Boolean = minor.isDevVersion

  override def compare(that: LanguageVersion): Int = {
    (this.major, this.minor).compare((that.major, that.minor))
  }
}

object LanguageVersion extends LanguageFeaturesGenerated {
  sealed abstract class Major(val major: Int)
      extends Product
      with Serializable
      with Ordered[Major] {
    val pretty = major.toString
    override def compare(that: Major): Int = this.pretty.compare(that.pretty)
  }

  object Major {
    case object V1 extends Major(1)
    case object V2 extends Major(2)

    private val allMajors = List(V1, V2)

    def fromString(str: String): Either[String, Major] =
      allMajors
        .find(_.pretty == str)
        .toRight(s"${str} is not supported, supported majors: ${allMajors}")
  }

  sealed abstract class Minor extends Product with Serializable with Ordered[Minor] {
    val toProtoIdentifier: String = pretty
    def pretty: String
    def isDevVersion: Boolean = false

    override def compare(that: Minor): Int = (this, that) match {
      // Dev > Staging > Stable
      case (Minor.Dev, Minor.Dev) => 0
      case (Minor.Dev, _) => 1
      case (_, Minor.Dev) => -1

      case (Minor.Staging(a), Minor.Staging(b)) => a.compare(b)
      case (Minor.Staging(_), Minor.Stable(_)) => 1
      case (Minor.Stable(_), Minor.Staging(_)) => -1

      case (Minor.Stable(a), Minor.Stable(b)) => a.compare(b)
    }
  }

  object Minor {
    final case class Stable(version: Int) extends Minor {
      override def pretty: String = version.toString
    }

    final case class Staging(version: Int) extends Minor {
      override def pretty: String = s"${version}-staging"
    }

    case object Dev extends Minor {
      override def pretty: String = "dev"
      override def isDevVersion: Boolean = true
    }

    def fromString(str: String): Either[String, Minor] =
      (allLfVersions ++ allLegacyLfVersions)
        .map(_.minor)
        .find(_.pretty == str)
        .toRight(
          s"${str} is not supported, supported minors: ${(allLfVersions ++ allLegacyLfVersions).map(_.minor)}"
        )
    def assertFromString(s: String): Minor = data.assertRight(fromString(s))
  }

  final case class Feature(
      name: String,
      versionRange: VersionRange[LanguageVersion],
  ) {
    def enabledIn(lv: LanguageVersion): Boolean = versionRange.contains(lv)
  }

  def fromString(str: String): Either[String, LanguageVersion] =
    (allLegacyLfVersions ++ allLfVersions)
      .find(_.toString == str)
      .toRight(s"${str} is not supported")
  def assertFromString(s: String): LanguageVersion = data.assertRight(fromString(s))

  // TODO: remove after https://github.com/digital-asset/daml/issues/22403
  def supportsPackageUpgrades(lv: LanguageVersion): Boolean =
    lv.major match {
      case Major.V2 => featurePackageUpgrades.enabledIn(lv)
      case Major.V1 => lv >= LegacyFeatures.packageUpgrades
    }

  // TODO: remove after feature https://github.com/digital-asset/daml/issues/22403
  def allUpToVersion(version: LanguageVersion): VersionRange.Inclusive[LanguageVersion] = {
    version.major match {
      case Major.V2 => VersionRange(v2_1, version)
      case _ => throw new IllegalArgumentException(s"${version.major.pretty} not supported")
    }
  }
}
