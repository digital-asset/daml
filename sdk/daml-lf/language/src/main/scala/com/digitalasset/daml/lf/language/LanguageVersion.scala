// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package language

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

  private[this] lazy val stringToVersions = All.iterator.map(v => v.pretty -> v).toMap

  def fromString(s: String): Either[String, LanguageVersion] =
    stringToVersions.get(s).toRight(s + " is not supported")

  def assertFromString(s: String): LanguageVersion = data.assertRight(fromString(s))

  // TODO(#17366): As soon as LF2 introduces breaking changes w.r.t. LF1, this order will no longer
  //    be total and should be replaced by ad-hoc methods wherever it is used.
  implicit val Ordering: scala.Ordering[LanguageVersion] = {
    case (LanguageVersion(Major.V1, leftMinor), LanguageVersion(Major.V1, rightMinor)) =>
      Major.V1.minorVersionOrdering.compare(leftMinor, rightMinor)
    case (LanguageVersion(Major.V2, leftMinor), LanguageVersion(Major.V2, rightMinor)) =>
      Major.V2.minorVersionOrdering.compare(leftMinor, rightMinor)
    case (LanguageVersion(Major.V2, _), LanguageVersion(Major.V1, _)) =>
      1
    case (_, _) =>
      -1
  }

  val All = {
    val v1Versions = Major.V1.supportedMinorVersions.map(LanguageVersion(Major.V1, _))
    val v2Versions = Major.V2.supportedMinorVersions.map(LanguageVersion(Major.V2, _))
    v1Versions ++ v2Versions
  }

  val List(v1_6, v1_7, v1_8, v1_11, v1_12, v1_13, v1_14, v1_15, v1_dev, v2_dev) =
    All: @nowarn("msg=match may not be exhaustive")

  // TODO(#17366): Once LF2 deprecates some features, it will no longer be possible to represent
  //    them as a just a version number. Instead we'll need a richer specification of which versions
  //    support which feature. See PR #17334.
  object Features {
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
    val packageUpgrades = v1_dev
    val dynamicExercise = v1_dev
    val sharedKeys = v1_dev

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

  // All the stable versions.
  val StableVersions: VersionRange[LanguageVersion] =
    VersionRange(min = v1_6, max = v1_15)

  // All versions compatible with legacy contract ID scheme.
  val LegacyVersions: VersionRange[LanguageVersion] =
    StableVersions.copy(max = v1_8)

  // All the stable and preview versions
  // Equals `Stable` if no preview version is available
  val EarlyAccessVersions: VersionRange[LanguageVersion] =
    StableVersions

  // All the versions
  def AllVersions(majorLanguageVersion: LanguageMajorVersion): VersionRange[LanguageVersion] = {
    majorLanguageVersion match {
      case Major.V1 => EarlyAccessVersions.copy(max = v1_dev)
      case Major.V2 =>
        // TODO(#17366): change for 2.0-2.dev once 2.0 is introduced
        VersionRange(v2_dev, v2_dev)
    }
  }

// To temporarily preserve compatibility with Canton which creates an engine
  // for the range (1.14, DevVersions.max) when running in dev mode and doesn't
  // distinguish between LF1 and LF2. Usage in the daml repository is forbidden.
  // TODO(#17366): delete and get Canton to use AllVersions.
  @deprecated("use LanguageVersion.AllVersions", since = "2.8.0")
  def DevVersions: VersionRange[LanguageVersion] =
    VersionRange(v1_dev, v2_dev)

  // This refers to the default output LF version in the compiler
  val default: LanguageVersion = v1_14
}

/** Operations on [[VersionRange]] that only make sense for ranges of [[LanguageVersion]]. */
object LanguageVersionRangeOps {
  implicit class LanguageVersionRange(val range: VersionRange[LanguageVersion]) {
    def majorVersion: LanguageMajorVersion = {
      // TODO(#17366): uncomment once Canton stops using (1.14, 2.dev) as the version range for dev.
      // require(
      //  range.min.major == range.max.major,
      //  s"version range ${range} spans over multiple version LF versions")
      range.max.major
    }
  }
}
