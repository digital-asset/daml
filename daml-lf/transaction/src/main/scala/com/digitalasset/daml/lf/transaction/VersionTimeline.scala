// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.language.{LanguageVersion, LanguageMajorVersion => LMV}
import com.daml.lf.value.ValueVersion
import scalaz.std.map._
import scalaz.syntax.foldable1._
import scalaz.syntax.order._
import scalaz.syntax.std.option._
import scalaz.{-\/, @@, NonEmptyList, OneAnd, Ordering, Semigroup, \&/, \/, \/-}

import scala.language.higherKinds

// FIXME: https://github.com/digital-asset/daml/issues/7788
// VersionTimeline is currently being rewritten and simplified.
// Comments below may be inaccurate.

/** The "monotonically decreasing" guarantee of engine versioning
  * described by the LF governance rules implicitly permits us to
  * exploit the order in which versions happen to have been introduced.
  * That's important if we want to introduce improved structures and
  * have any hope of them actually appearing in engine-produced
  * messages.
  *
  * This timeline gives us a way to describe that knowledge in a
  * future-survivable way, i.e. you don't have to understand how the
  * version picker uses the timeline in order to describe changes to
  * that same timeline.
  */
object VersionTimeline {
  import LanguageVersion.Minor.Dev
  import \&/.{Both, That, This}

  type AllVersions[:&:[_, _]] = (ValueVersion :&: TransactionVersion) :&: LanguageVersion
  type Release = AllVersions[\&/]

  /** If a version occurs at an earlier index than another version in this list,
    * it appeared in an earlier engine release.  If two versions occur at the
    * same index, they were both added in the same engine release.
    */
  private[lf] val inAscendingOrder: NonEmptyList[Release] =
    NonEmptyList(
      This(This(ValueVersion("1"))),
      This(This(ValueVersion("2"))),
      This(This(ValueVersion("3"))),
      This(This(ValueVersion("4"))),
      Both(This(ValueVersion("5")), LanguageVersion(LMV.V1, "6")),
      Both(This(ValueVersion("6")), LanguageVersion(LMV.V1, "7")),
      That(LanguageVersion(LMV.V1, "8")),
      This(That(TransactionVersion("10"))),
      // add new versions above this line (but see more notes below)
      Both(Both(ValueVersion("dev"), TransactionVersion("dev")), LanguageVersion(LMV.V1, Dev)),
      //
      // "dev" versions float through the timeline with little rationale
      // due to their ephemeral contents; don't worry too much about their exact
      // positioning, except where you desire a temporal implication between dev
      // and some other version you're introducing. Dev always means "the dev
      // supported by this release".
    )

  private[lf] def foldRelease[Z: Semigroup](
      av: Release,
  )(v: ValueVersion => Z, t: TransactionVersion => Z, l: LanguageVersion => Z): Z =
    av.bifoldMap(_.bifoldMap(v)(t))(l)

  final case class SubVersion[A](inject: A => SpecifiedVersion, extract: Release => Option[A])
  object SubVersion {
    implicit def value: SubVersion[ValueVersion] =
      SubVersion(vv => -\/(-\/(vv)), _.a.flatMap(_.a))
    implicit def transaction: SubVersion[TransactionVersion] =
      SubVersion(vv => -\/(\/-(vv)), _.a.flatMap(_.b))
    implicit def language: SubVersion[LanguageVersion] =
      SubVersion(\/.right, _.b)
  }

  type SpecifiedVersion = AllVersions[\/]
  def SpecifiedVersion(sv: SpecifiedVersion): sv.type = sv

  object Implicits {
    import scala.language.implicitConversions
    implicit def `any to SV`[A](vv: A)(implicit ev: SubVersion[A]): SpecifiedVersion =
      ev.inject(vv)

    implicit final class SpecifiedVersionOps(private val sv: SpecifiedVersion) extends AnyVal {
      def foldVersion[Z](
          v: ValueVersion => Z,
          t: TransactionVersion => Z,
          l: LanguageVersion => Z,
      ): Z =
        sv fold (_ fold (v, t), l)

      private[lf] def showsVersion: String = foldVersion(_.toString, _.toString, _.toString)

      private[lf] def precedes(ov: SpecifiedVersion): Boolean = releasePrecedes(sv, ov)
    }

    implicit def `any to SVOps`[A: SubVersion](vv: A): SpecifiedVersionOps =
      vv: SpecifiedVersion
  }

  /** Inversion of [[inAscendingOrder]]. */
  private val index: Map[SpecifiedVersion, Int] = {
    import Implicits._
    import scalaz.Tags.FirstVal
    implicit val combineInts: Semigroup[Int] =
      FirstVal.unsubst(Semigroup[Int @@ FirstVal])
    inAscendingOrder.zipWithIndex foldMap1 {
      case (avb, ix) =>
        foldRelease(avb)(
          vv => Map((SpecifiedVersion(vv), ix)),
          tv => Map((SpecifiedVersion(tv), ix)),
          lv => Map((SpecifiedVersion(lv), ix)),
        )
    }
  }

  /** The relative position in the release timeline of `left` and `right`.
    *
    * @note We do not know the relative ordering of unlisted versions; so
    *       the meaning of "no index" is not "equal" but undefined.
    */
  private[lf] def compareReleaseTime(
      left: SpecifiedVersion,
      right: SpecifiedVersion): Option[Ordering] =
    (index get left, index get right) match {
      case (Some(ixl), Some(ixr)) =>
        import scalaz.std.anyVal._
        Some(ixl ?|? ixr)
      case (Some(_), None) => Some(Ordering.LT)
      case (None, Some(_)) => Some(Ordering.GT)
      case (None, None) => None
    }

  private def releasePrecedes(left: SpecifiedVersion, right: SpecifiedVersion): Boolean =
    compareReleaseTime(left, right) contains Ordering.LT

  /** Released versions in ascending order.  Public clients should prefer
    * `ValueVersions` and `TransactionVersions`' members.
    */
  private[lf] def ascendingVersions[A](implicit A: SubVersion[A]): NonEmptyList[A] =
    inAscendingOrder.list
      .collect(Function unlift A.extract)
      .toNel
      .getOrElse(sys.error("every SubVersion must have at least one entry in the timeline"))

  // not antisymmetric, as unknown versions can't be compared
  private[lf] def maxVersion[A](left: A, right: A)(implicit ev: SubVersion[A]): A =
    if (releasePrecedes(ev.inject(left), ev.inject(right))) right else left

  // not antisymmetric, as unknown versions can't be compared
  private[lf] def minVersion[A](left: A, right: A)(implicit ev: SubVersion[A]): A =
    if (releasePrecedes(ev.inject(left), ev.inject(right))) left else right

  private[lf] def latestWhenAllPresent[A](minimum: A, as: SpecifiedVersion*)(
      implicit A: SubVersion[A]): A = {
    import scalaz.std.anyVal._
    import scalaz.std.iterable._
    // None means "after the end"
    val latestIndex: Option[Int] = OneAnd(A.inject(minimum), as)
      .maximumOf1(sv => index.get(sv).cata(\/.left, \/-(())))
      .swap
      .toOption
    latestIndex
      .flatMap(li =>
        inAscendingOrder.list.take(li + 1).reverse collectFirst (Function unlift A.extract))
      .getOrElse(minimum)
  }

  private[lf] val stableLanguageVersions =
    VersionRange(
      min = LanguageVersion(LMV.V1, "6"),
      max = LanguageVersion(LMV.V1, "8"),
    )

  private[lf] val devLanguageVersions =
    stableLanguageVersions.copy(max = LanguageVersion(LMV.V1, Dev))

}
