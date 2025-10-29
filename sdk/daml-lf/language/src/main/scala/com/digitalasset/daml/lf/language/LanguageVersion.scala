// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package language

import com.digitalasset.daml.lf.language.LanguageMajorVersion.V2.maxStableVersion

import scala.annotation.nowarn

final case class LanguageVersion(major: LanguageMajorVersion, minor: LanguageMinorVersion) {
  def pretty: String = s"${major.pretty}.${minor.toProtoIdentifier}"

  def isDevVersion = minor.identifier == "dev"
}

object LanguageVersion {

  type Major = LanguageMajorVersion
  val Major = LanguageMajorVersion

  type Minor = LanguageMinorVersion
  val Minor = LanguageMinorVersion

  private[this] lazy val stringToVersions = (AllV1 ++ AllV2).iterator.map(v => v.pretty -> v).toMap

  def fromString(s: String): Either[String, LanguageVersion] =
    stringToVersions.get(s).toRight(s + " is not supported")

  def assertFromString(s: String): LanguageVersion = data.assertRight(fromString(s))

  // TODO https://github.com/digital-asset/daml/issues/20101
  //   Drop the ordering
  implicit val Ordering: scala.Ordering[LanguageVersion] = {
    case (LanguageVersion(Major.V1, leftMinor), LanguageVersion(Major.V1, rightMinor)) =>
      Major.V1.minorVersionOrdering.compare(leftMinor, rightMinor)
    case (LanguageVersion(Major.V1, _), LanguageVersion(Major.V2, _)) =>
      -1
    case (LanguageVersion(Major.V2, _), LanguageVersion(Major.V1, _)) =>
      1
    case (LanguageVersion(Major.V2, leftMinor), LanguageVersion(Major.V2, rightMinor)) =>
      Major.V2.minorVersionOrdering.compare(leftMinor, rightMinor)
  }

  val AllV1 = Major.V1.supportedMinorVersions.map(LanguageVersion(Major.V1, _))
  val AllV2 = Major.V2.supportedMinorVersions.map(LanguageVersion(Major.V2, _))

  val List(v1_6, v1_7, v1_8, v1_11, v1_12, v1_13, v1_14, v1_15, v1_17, v1_dev) = AllV1: @nowarn(
    "msg=match may not be exhaustive"
  )
  val List(v2_1, v2_2, v2_dev) = AllV2: @nowarn("msg=match may not be exhaustive")

  @deprecated("use AllV2", since = "3.1.0")
  val All = AllV2

  private def supportsV1AndV2(
      lv: LanguageVersion,
      minorV2: Option[Minor],
      minorV1: Option[Minor],
  ): Boolean = {
    import scala.math.Ordering.Implicits._
    lv.major match {
      case Major.V2 =>
        minorV2 match {
          case Some(minorV2) => lv >= LanguageVersion(Major.V2, minorV2)
          case None => false
        }
      case Major.V1 =>
        minorV1 match {
          case Some(minorV1) => lv >= LanguageVersion(Major.V1, minorV1)
          case None => false
        }
    }
  }

  def supportsPackageUpgrades(lv: LanguageVersion): Boolean =
    supportsV1AndV2(
      lv,
      Some(Features.packageUpgrades.minor),
      Some(FeaturesV1.packageUpgrades.minor),
    )

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

  object FeaturesV1 {
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

    /** TYPE_REP_TYCON_NAME builtin */
    val templateTypeRepToText = v1_dev

    /** Guards in interfaces */
    val extendedInterfaces = v1_dev

    /** Unstable, experimental features. This should stay in x.dev forever.
      * Features implemented with this flag should be moved to a separate
      * feature flag once the decision to add them permanently has been made.
      */
    val unstable = v1_dev
  }

  private[lf] def notSupported(majorLanguageVersion: LanguageMajorVersion) =
    throw new IllegalArgumentException(s"${majorLanguageVersion.pretty} not supported")

  /** All the stable versions for a given major language version.
    * Version ranges don't make sense across major language versions because major language versions
    * break backwards compatibility. Clients of [[VersionRange]] in the codebase assume that all LF
    * versions in a range are backwards compatible with the older versions within that range. Hence
    * the majorLanguageVersion parameter.
    */
  def StableVersions(majorLanguageVersion: LanguageMajorVersion): VersionRange[LanguageVersion] =
    majorLanguageVersion match {
      case Major.V2 => VersionRange(v2_1, v2_2)
      case _ => notSupported(majorLanguageVersion)
    }

  /** All the stable and preview versions for a given major language version.
    * Equals [[StableVersions(majorLanguageVersion)]] if no preview version is available.
    */
  def EarlyAccessVersions(
      majorLanguageVersion: LanguageMajorVersion
  ): VersionRange[LanguageVersion] =
    StableVersions(majorLanguageVersion)

  /** All the supported versions for a given major language version: stable, early access and dev.
    */
  def AllVersions(majorLanguageVersion: LanguageMajorVersion): VersionRange[LanguageVersion] = {
    majorLanguageVersion match {
      case Major.V2 => VersionRange(v2_1, v2_dev)
      case _ => notSupported(majorLanguageVersion)
    }
  }

  /** The Daml-LF version used by default by the compiler if it matches the
    * provided major version, the latest non-dev version with that major version
    * otherwise. This function is meant to be used in tests who want to test the
    * closest thing to the default user experience given a major version.
    */
  def defaultOrLatestStable(majorLanguageVersion: LanguageMajorVersion): LanguageVersion = {
    majorLanguageVersion match {
      case Major.V2 => maxStableVersion
      case _ => notSupported(majorLanguageVersion)
    }
  }

  /** Version range including the passed one */
  def allUpToVersion(version: LanguageVersion): VersionRange[LanguageVersion] = {
    version.major match {
      case Major.V2 => VersionRange(v2_1, version)
      case _ => notSupported(version.major)
    }
  }

  // This refers to the default output LF version in the compiler
  val default: LanguageVersion = defaultOrLatestStable(Major.V2)
}

/** Operations on [[VersionRange]] that only make sense for ranges of [[LanguageVersion]]. */
object LanguageVersionRangeOps {
  implicit class LanguageVersionRange(val range: VersionRange[LanguageVersion]) {
    def majorVersion: LanguageMajorVersion = {
      require(
        range.min.major == range.max.major,
        s"version range ${range} spans over multiple version LF versions",
      )
      range.max.major
    }
  }
}


object New {
  final case class LanguageVersion private(
                                            major: LanguageVersion.LegacyMajor,
                                            minor: LanguageVersion.Minor
                                          ) extends Ordered[LanguageVersion] {

    override def pretty: String = s"${major.pretty}.${minor.pretty}"

    override def compare(that: LanguageVersion): Int = {
      (this.major, this.minor, that.major, that.minor) match {
        // Dev is highest
        case (LanguageVersion.Major.V2, LanguageVersion.Minor.Dev, LanguageVersion.Major.V2, LanguageVersion.Minor.Dev) => 0
        case (LanguageVersion.Major.V2, LanguageVersion.Minor.Dev, LanguageVersion.Major.V2, _) => 1
        case (LanguageVersion.Major.V2, _, LanguageVersion.Major.V2, LanguageVersion.Minor.Dev) => -1

        // Stable vs Stable
        case (LanguageVersion.Major.V2, LanguageVersion.Minor.Stable(a), LanguageVersion.Major.V2, LanguageVersion.Minor.Stable(b)) => a.compare(b)

        case _ => 0 // Should be unreachable with current bzl data
      }
    }
  }

  object LanguageVersion {
    sealed abstract class LegacyMajor(val pretty: String) extends Product with Serializable

    sealed abstract class Major(pretty: String) extends LegacyMajor(pretty) with Serializable {
    }

    object Major {
      case object V1 extends LegacyMajor("1")
      case object V2 extends Major("2") {

        //override def fromString1(str: String): Option[Major] = str match {
        //  case "2" => Some(V2)
        //  case _   => None
        //}
      }
    }

    sealed abstract class Minor extends Product with Serializable {
      def pretty: String
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
      }
    }

    val allLegacy = List(6, 7, 8, 11, 12, 13, 14, 15, 17).map(i => LanguageVersion(Major.V1, Minor.Stable(i)))
    val List(v1_6, v1_7, v1_8, v1_11, v1_12, v1_13, v1_14, v1_15, v1_17, v1_dev) = allLegacy

    // --- Start of Generated Code ---
    val v2_1: LanguageVersion = LanguageVersion(Major.V2, Minor.Stable(1))
    val v2_2: LanguageVersion = LanguageVersion(Major.V2, Minor.Stable(2))
    val v2_dev: LanguageVersion = LanguageVersion(Major.V2, Minor.Dev)

    val latestStable: LanguageVersion = v2_2
    val defaultVersion: LanguageVersion = v2_2
    val dev: LanguageVersion = v2_dev
    val staging: LanguageVersion = v2_2

    val all: List[LanguageVersion] = List(v2_1, v2_2, v2_dev)
    val stable: List[LanguageVersion] = List(v2_1, v2_2)
    val compilerInput: List[LanguageVersion] = List(v2_1, v2_2, v2_dev)
    val compilerOutput: List[LanguageVersion] = List(v2_1, v2_2, v2_dev)
    // --- End of Generated Code ---

    def fromString(str: String): Either[String, LanguageVersion] =
      (allLegacy ++ all).find(_.toString == str).toRight(s"${str} is not supported")

    def assertFromString(s: String): LanguageVersion = data.assertRight(fromString(s))

    // --- Backwards-compoatible definitions ---
    private[lf] def notSupported(major: Major) =
      throw new IllegalArgumentException(s"${major.pretty} not supported")

    @deprecated
    def supportsPackageUpgrades(lv: LanguageVersion): Boolean =
      lv.major match {
        case Major.V2 => lv >= Features.packageUpgrades
        case Major.V1 => lv >= LegacyFeatures.packageUpgrades
      }

    @deprecated
    def StableVersions(major: Major): VersionRange[LanguageVersion] =
      major match {
        case Major.V2 => VersionRange(stable.head, stable.last)
        case _ => notSupported(major)
      }

    @deprecated
    def EarlyAccessVersions(major: Major): VersionRange[LanguageVersion] =
      StableVersions(major)

    @deprecated
    def AllVersions(major: Major): VersionRange[LanguageVersion] = {
      major match {
        case Major.V2 => VersionRange(all.head, all.last)
        case _ => notSupported(major)
      }
    }

    @deprecated
    def defaultOrLatestStable(major: Major): LanguageVersion = {
      major match {
        case Major.V2 => latestStable
        case _ => notSupported(major)
      }
    }

    @deprecated
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


}