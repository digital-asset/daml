// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.util.VersionUtil

import scala.annotation.tailrec
import scala.util.Try

/** Trait that represents how a version in Canton is modelled. */
sealed trait CantonVersion extends Ordered[CantonVersion] with PrettyPrinting {

  def major: Int
  def minor: Int
  def patch: Int
  def optSuffix: Option[String]
  def isSnapshot: Boolean = optSuffix.contains("SNAPSHOT")
  def isStable: Boolean = optSuffix.isEmpty
  def fullVersion: String = s"$major.$minor.$patch${optSuffix.map("-" + _).getOrElse("")}"

  override def pretty: Pretty[CantonVersion] = prettyOfString(_ => fullVersion)
  def toProtoPrimitive: String = fullVersion

  def raw: (Int, Int, Int, Option[String]) = (major, minor, patch, optSuffix)

  /* Compared according to the SemVer specification: https://semver.org/#spec-item-11
  (implementation was only tested when specifying a pre-release suffix, see `CantonVersionTest`, but not e.g. with a metadata suffix). */
  override def compare(that: CantonVersion): Int =
    if (this.major != that.major) this.major compare that.major
    else if (this.minor != that.minor) this.minor compare that.minor
    else if (this.patch != that.patch) this.patch compare that.patch
    else suffixComparison(this.optSuffix, that.optSuffix)

  def suffixComparison(maybeSuffix1: Option[String], maybeSuffix2: Option[String]): Int = {
    (maybeSuffix1, maybeSuffix2) match {
      case (None, None) => 0
      case (None, Some(_)) => 1
      case (Some(_), None) => -1
      case (Some(suffix1), Some(suffix2)) =>
        suffixComparisonInternal(suffix1.split("\\.").toSeq, suffix2.split("\\.").toSeq)
    }
  }

  private def suffixComparisonInternal(suffixes1: Seq[String], suffixes2: Seq[String]): Int = {
    // partially adapted (and generalised) from gist.github.com/huntc/35f6cec0a47ce7ef62c0 (Apache 2 license)
    type PreRelease = Either[String, Int]
    def toPreRelease(s: String): PreRelease = Try(Right(s.toInt)).getOrElse(Left(s))

    def comparePreReleases(preRelease: PreRelease, thatPreRelease: PreRelease): Int =
      (preRelease, thatPreRelease) match {
        case (Left(str1), Left(str2)) =>
          str1 compare str2
        case (Left(_), Right(_)) =>
          1
        case (Right(number1), Right(number2)) =>
          number1 compare number2
        case (Right(_), Left(_)) =>
          -1
      }

    @tailrec
    def go(
        suffix1: Option[String],
        suffix2: Option[String],
        tail1: Seq[String],
        tail2: Seq[String],
    ): Int = {
      (suffix1, suffix2) match {
        case (None, None) => 0
        // if we have a suffix (else we would have terminated earlier), then more suffixes are better
        case (None, Some(_)) => -1
        case (Some(_), None) => 1
        case (Some(suffix1), Some(suffix2)) =>
          val res = comparePreReleases(toPreRelease(suffix1), toPreRelease(suffix2))
          if (res != 0) res
          else go(tail1.headOption, tail2.headOption, tail1.drop(1), tail2.drop(1))
      }
    }

    go(suffixes1.headOption, suffixes2.headOption, suffixes1.drop(1), suffixes2.drop(1))
  }
}

/** This class represent a release version.
  * Please refer to the [[https://docs.daml.com/canton/usermanual/versioning.html versioning documentation]]
  * in the user manual for details.
  */
final case class ReleaseVersion(
    major: Int,
    minor: Int,
    patch: Int,
    optSuffix: Option[String] = None,
) extends CantonVersion {
  def majorMinor: (Int, Int) = (major, minor)

  def majorMinorMatches(other: ReleaseVersion): Boolean =
    major == other.major && minor == other.minor

}

object ReleaseVersion {

  def create(rawVersion: String): Either[String, ReleaseVersion] =
    VersionUtil.create(rawVersion, ReleaseVersion.getClass.getSimpleName).map {
      case (major, minor, patch, optSuffix) =>
        new ReleaseVersion(major, minor, patch, optSuffix)
    }
  def tryCreate(rawVersion: String): ReleaseVersion = create(rawVersion).fold(sys.error, identity)

  /** The release this process belongs to. */
  val current: ReleaseVersion = ReleaseVersion.tryCreate(BuildInfo.version)
}
