// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.language.{LanguageVersion, LanguageMajorVersion => LMV}
import com.digitalasset.daml.lf.value.ValueVersion
import scalaz.std.map._
import scalaz.syntax.foldable1._
import scalaz.syntax.order._
import scalaz.syntax.std.option._
import scalaz.{-\/, @@, NonEmptyList, OneAnd, Ordering, Semigroup, \&/, \/, \/-}

import scala.language.higherKinds

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
private[digitalasset] object VersionTimeline {
  import LanguageVersion.Minor.Dev
  import \&/.{Both, That, This}

  type AllVersions[:&:[_, _]] = (ValueVersion :&: TransactionVersion) :&: LanguageVersion
  type Release = AllVersions[\&/]

  /** If a version occurs at an earlier index than another version in this list,
    * it appeared in an earlier engine release.  If two versions occur at the
    * same index, they were both added in the same engine release.
    */
  private[transaction] val inAscendingOrder: NonEmptyList[Release] =
    NonEmptyList(
      That(LanguageVersion(LMV.V0, "")),
      That(LanguageVersion(LMV.V0, Dev)),
      Both(Both(ValueVersion("1"), TransactionVersion("1")), LanguageVersion(LMV.V1, "0")),
      Both(Both(ValueVersion("2"), TransactionVersion("2")), LanguageVersion(LMV.V1, "1")),
      This(That(TransactionVersion("3"))),
      This(Both(ValueVersion("3"), TransactionVersion("4"))),
      This(That(TransactionVersion("5"))),
      That(LanguageVersion(LMV.V1, "2")),
      Both(This(ValueVersion("4")), LanguageVersion(LMV.V1, "3")),
      This(That(TransactionVersion("6"))),
      This(That(TransactionVersion("7"))),
      That(LanguageVersion(LMV.V1, "4")),
      That(LanguageVersion(LMV.V1, "5")),
      This(That(TransactionVersion("8"))),
      Both(This(ValueVersion("5")), LanguageVersion(LMV.V1, "6")),
      Both(This(ValueVersion("6")), LanguageVersion(LMV.V1, "7")),
      This(That(TransactionVersion("9"))),
      // FIXME https://github.com/digital-asset/daml/issues/2256
      //  * change the following line when LF 1.8 is frozen.
      //  * do not insert line after this once until 1.8 is frozen.
      This(This(ValueVersion("7"))),
      // add new versions above this line (but see more notes below)
      That(LanguageVersion(LMV.V1, Dev)),
      // do *not* backfill to make more Boths, because such would
      // invalidate the timeline, except to accompany Dev language
      // versions; use This and That instead as needed.
      // Backfill *is* appropriate if a release of the last hasn't happened
      //
      // "dev" versions float through the timeline with little rationale
      // due to their ephemeral contents; don't worry too much about their exact
      // positioning, except where you desire a temporal implication between dev
      // and some other version you're introducing. Dev always means "the dev
      // supported by this release".
    )

  def foldRelease[Z: Semigroup](
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

      def showsVersion: String = foldVersion(_.toString, _.toString, _.toString)

      def precedes(ov: SpecifiedVersion): Boolean = releasePrecedes(sv, ov)
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
  def compareReleaseTime(left: SpecifiedVersion, right: SpecifiedVersion): Option[Ordering] =
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
  def maxVersion[A](left: A, right: A)(implicit ev: SubVersion[A]): A =
    if (releasePrecedes(ev.inject(left), ev.inject(right))) right else left

  def latestWhenAllPresent[A](minimum: A, as: SpecifiedVersion*)(implicit A: SubVersion[A]): A = {
    import scalaz.std.anyVal._
    import scalaz.std.iterable._
    // None means "after the end"
    val latestIndex: Option[Int] = OneAnd(A.inject(minimum), as)
      .maximumOf1(sv => index.get(sv).cata(\/.left, \/-(())))
      .swap
      .toOption
    latestIndex
      .flatMap(li =>
        inAscendingOrder.list.take(li + 1).reverse collectFirst (Function unlift A.extract),
      )
      .getOrElse(minimum)
  }

  def checkSubmitterInMaintainers(lfVers: LanguageVersion): Boolean = {
    import Implicits._
    !(lfVers precedes LanguageVersion.Features.checkSubmitterInMaintainersVersion)
  }

}
