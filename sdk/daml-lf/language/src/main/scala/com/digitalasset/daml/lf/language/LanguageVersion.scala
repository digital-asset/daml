// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package language

import scala.annotation.nowarn

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

object LanguageVersion {
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
    // TODO: remove alias
    val identifier: String = pretty
    def pretty: String
    def isDevVersion: Boolean = false

    override def compare(that: Minor): Int = (this, that) match {
      // Dev is the highest version
      case (Minor.Dev, Minor.Dev) => 0
      case (Minor.Dev, _) => 1
      case (_, Minor.Dev) => -1

      // Staging is the next highest
      case (Minor.Staging(a), Minor.Staging(b)) => a.compare(b)
      case (Minor.Staging(_), Minor.Stable(_)) => 1
      case (Minor.Stable(_), Minor.Staging(_)) => -1

      // Stable is the lowest
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

    // TODO: make this less hardcode-y
    def fromString(input: String): Minor = {
      input match {
        // "dev" case
        case "dev" => Dev

        // All stable int cases
        case "1" => Stable(1)
        case "2" => Stable(2)
        case "6" => Stable(6)
        case "7" => Stable(7)
        case "8" => Stable(8)
        case "11" => Stable(11)
        case "12" => Stable(12)
        case "13" => Stable(13)
        case "14" => Stable(14)
        case "15" => Stable(15)
        case "17" => Stable(17)

        // All other cases throw an exception
        case _ =>
          throw new IllegalArgumentException(s"Invalid language version string: '$input'")
      }
    }
  }

  val allStableLegacy: List[LanguageVersion] =
    List(6, 7, 8, 11, 12, 13, 14, 15, 17).map(i => LanguageVersion(Major.V1, Minor.Stable(i)))
  val List(v1_6, v1_7, v1_8, v1_11, v1_12, v1_13, v1_14, v1_15, v1_17) = allStableLegacy: @nowarn(
    "msg=match may not be exhaustive"
  )
  val v1_dev: LanguageVersion = LanguageVersion(Major.V1, Minor.Dev)
  val allLegacy: List[LanguageVersion] = allStableLegacy.appended(v1_dev)

  // --- Start of Generated Code ---
  val v2_1: LanguageVersion = LanguageVersion(Major.V2, Minor.Stable(1))
  val v2_2: LanguageVersion = LanguageVersion(Major.V2, Minor.Stable(2))
  val v2_dev: LanguageVersion = LanguageVersion(Major.V2, Minor.Dev)

  val latestStable: LanguageVersion = v2_2
  val default: LanguageVersion = v2_2
  val dev: LanguageVersion = v2_dev

  val all: List[LanguageVersion] = List(v2_1, v2_2, v2_dev)
  val stable: List[LanguageVersion] = List(v2_1, v2_2)
  val compilerInput: List[LanguageVersion] = List(v2_1, v2_2, v2_dev)
  val compilerOutput: List[LanguageVersion] = List(v2_1, v2_2, v2_dev)
  // --- End of Generated Code ---

  def fromString(str: String): Either[String, LanguageVersion] =
    (allLegacy ++ all).find(_.toString == str).toRight(s"${str} is not supported")

  def assertFromString(s: String): LanguageVersion = data.assertRight(fromString(s))

  // --- Helper functions --
  implicit class LanguageVersionListOps(val list: List[LanguageVersion]) extends AnyVal {

    /** Extracts a list of all Minor versions from a list of LanguageVersions.
      */
    def toRange: VersionRange[LanguageVersion] = {
      VersionRange(list.head, list.last)
    }
  }

  // @deprecated("Actually not sure if deprecated", since="3.5")
  object LanguageVersionRangeOps {
    implicit class LanguageVersionRange(val range: VersionRange[LanguageVersion]) {
      def majorVersion: Major = {
        require(
          range.min.major == range.max.major,
          s"version range ${range} spans over multiple version LF versions",
        )
        range.max.major
      }
    }
  }

  // --- Backwards-compoatible definitions ---
  // @deprecated("Version rework, other reason", since="3.5")
  val Ordering: scala.Ordering[LanguageVersion] =
    (a, b) => a.compare(b)

  // @deprecated("Version rework, use generated variables instead", since="3.5")
  private[lf] def notSupported(major: Major) =
    throw new IllegalArgumentException(s"${major.pretty} not supported")

  // @deprecated("Version rework, use generated variables instead", since="3.5")
  def AllV1: Seq[LanguageVersion] = allLegacy
  def AllV2: Seq[LanguageVersion] = all

  // @deprecated("Version rework, use generated variables instead", since="3.5")
  def supportsPackageUpgrades(lv: LanguageVersion): Boolean =
    lv.major match {
      case Major.V2 => lv >= Features.packageUpgrades
      case Major.V1 => lv >= LegacyFeatures.packageUpgrades
    }

  // @deprecated("Version rework, use generated variables instead", since="3.5")
  def StableVersions(major: Major): VersionRange[LanguageVersion] =
    major match {
      case Major.V2 => VersionRange(stable.head, stable.last)
      case _ => notSupported(major)
    }

  // @deprecated("Version rework, use generated variables instead", since="3.5")
  def EarlyAccessVersions(major: Major): VersionRange[LanguageVersion] =
    StableVersions(major)

  // @deprecated("Version rework, use generated variables instead", since="3.5")
  def AllVersions(major: Major): VersionRange[LanguageVersion] = {
    major match {
      case Major.V2 => VersionRange(all.head, all.last)
      case _ => notSupported(major)
    }
  }

  // @deprecated("Version rework, use generated variables instead", since="3.5")
  def defaultOrLatestStable(major: Major): LanguageVersion = {
    major match {
      case Major.V2 => latestStable
      case _ => notSupported(major)
    }
  }

  // @deprecated("Version rework, use generated variables instead", since="3.5")
  def allUpToVersion(version: LanguageVersion): VersionRange[LanguageVersion] = {
    version.major match {
      case Major.V2 => VersionRange(v2_1, version)
      case _ => notSupported(version.major)
    }
  }

  // --- Features ---
  object Features {
    val default = v2_1
    val packageUpgrades = v2_1

    val flatArchive = v2_2
    val kindInterning = flatArchive
    val exprInterning = flatArchive

    val explicitPkgImports = v2_2

    val choiceFuncs = v2_dev
    val choiceAuthority = v2_dev

    /** TYPE_REP_TYCON_NAME builtin */
    val templateTypeRepToText = v2_dev

    /** Guards in interfaces */
    val extendedInterfaces = v2_dev

    /** BigNumeric */
    val bigNumeric = v2_dev

    val contractKeys = v2_dev

    val complexAnyType = v2_dev

    val cryptoUtility = v2_dev

    /** UNSAFE_FROM_INTERFACE is removed starting from 2.2, included */
    val unsafeFromInterfaceRemoved = v2_2

    /** Unstable, experimental features. This should stay in x.dev forever.
      * Features implemented with this flag should be moved to a separate
      * feature flag once the decision to add them permanently has been made.
      */
    val unstable = v2_dev
  }

  object LegacyFeatures {
    val default = v1_6
    val internedPackageId = v1_6
    val internedStrings = v1_7
    val internedDottedNames = v1_7
    val numeric = v1_7
    val anyType = v1_7
    val typeRep = v1_7
    val typeSynonyms = v1_8
    val packageMetadata = v1_8
    val genComparison = v1_11
    val genMap = v1_11
    val scenarioMustFailAtMsg = v1_11
    val contractIdTextConversions = v1_11
    val exerciseByKey = v1_11
    val internedTypes = v1_11
    val choiceObservers = v1_11
    val bigNumeric = v1_13
    val exceptions = v1_14
    val basicInterfaces = v1_15
    val choiceFuncs = v1_dev
    val choiceAuthority = v1_dev
    val natTypeErasure = v1_dev
    val packageUpgrades = v1_17
    val sharedKeys = v1_17
    val templateTypeRepToText = v1_dev
    val extendedInterfaces = v1_dev
    val unstable = v1_dev
  }
}
