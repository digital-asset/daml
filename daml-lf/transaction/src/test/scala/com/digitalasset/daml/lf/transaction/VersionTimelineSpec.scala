// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.language._
import value.ValueVersions

import scala.language.higherKinds
import scalaz.{ICons, INil, NonEmptyList}
import scalaz.std.list._
import scalaz.syntax.foldable._
import org.scalacheck.Gen
import org.scalatest.{Inside, Matchers, WordSpec}
import org.scalatest.prop.PropertyChecks
import scalaz.\&/.That

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class VersionTimelineSpec extends WordSpec with Matchers with PropertyChecks with Inside {
  import VersionTimeline._
  import VersionTimelineSpec._

  "timeline versions" should {
    "match value versions in order" in {
      inAscendingOrder foldMap {
        foldRelease(_)(List(_), constNil, constNil)
      } shouldBe ValueVersions.acceptedVersions
    }

    "match transaction versions in order" in {
      inAscendingOrder foldMap {
        foldRelease(_)(constNil, List(_), constNil)
      } shouldBe TransactionVersions.acceptedVersions
    }

    val languageMajorSeries =
      Table(
        "major version",
        LanguageMajorVersion.All: _*,
      )

    "match each language major series in order" in forEvery(languageMajorSeries) { major =>
      val minorInAscendingOrder = inAscendingOrder foldMap {
        foldRelease(_)(constNil, constNil, {
          case LanguageVersion(`major`, minor) => List(minor)
          case _ => Nil
        })
      }

      unique(minorInAscendingOrder) shouldBe major.acceptedVersions
    }

    "be inlined with LanguageVersion.ordering" in {
      import VersionTimeline.Implicits._

      val versions = Table(
        "language version",
        LanguageMajorVersion.All.flatMap(major =>
          major.acceptedVersions.map(LanguageVersion(major, _)),
        ): _*,
      )

      forEvery(versions)(v1 =>
        forEvery(versions)(v2 => LanguageVersion.ordering.lt(v1, v2) shouldBe (v1 precedes v2)),
      )
    }

    "end with a dev version" in {
      inside(inAscendingOrder.last) {
        case That(LanguageVersion(_, LanguageMinorVersion.Dev)) =>
      }
    }

  }

  "compareReleaseTime" when {
    import scalaz.Ordering._, Implicits._
    "given defined versions" should {
      "be defined" in forAll(genSpecifiedVersion, genSpecifiedVersion) { (l, r) =>
        compareReleaseTime(l, r) shouldBe 'defined
      }
    }

    "given a dev version" should {
      "never precede another version of same major" in forAll(genDefinedLanguageVersion) { lv =>
        compareReleaseTime(lv, lv copy (minor = LanguageVersion.Minor.Dev)) should contain oneOf (LT, EQ)
      }
    }
  }

  "latestWhenAllPresent" when {
    "given only one version" should {
      "give it back" in forAll(genLatestInput) { easva =>
        latestWhenAllPresent(easva.run._1)(easva.run._2) shouldBe easva.run._1
      }

      "give it back reflexively" in forAll(genLatestInput) { easva =>
        val sv = easva.run._1
        implicit val ev: SubVersion[easva.T] = easva.run._2
        import Implicits._
        latestWhenAllPresent(sv, sv, sv, sv) shouldBe sv
      }
    }

    "given many versions" should {
      "give back one less-or-equal to some arg" in forAll(
        genLatestInput,
        Gen.listOf(genSpecifiedVersion),
      ) { (easva, svs) =>
        val sv = easva.run._1
        implicit val ev: SubVersion[easva.T] = easva.run._2
        val result = latestWhenAllPresent(sv, svs: _*)
        import Implicits._
        ((sv: SpecifiedVersion) :: svs).map(_ precedes result) should contain(false)
      }

      "be idempotent" in forAll(genLatestInput, Gen.listOf(genSpecifiedVersion)) { (easva, svs) =>
        val sv = easva.run._1
        implicit val ev: SubVersion[easva.T] = easva.run._2
        val result1 = latestWhenAllPresent(sv, svs: _*)
        latestWhenAllPresent(result1, svs: _*) shouldBe result1
      }

      "produce the first argument at minimum" in forAll(
        genLatestInput,
        Gen.listOf(genSpecifiedVersion),
      ) { (easva, svs) =>
        val sv = easva.run._1
        implicit val ev: SubVersion[easva.T] = easva.run._2
        import scalaz.Ordering._, Implicits._
        compareReleaseTime(sv, latestWhenAllPresent(sv, svs: _*)) should (be(Some(LT)) or be(
          Some(EQ),
        ))
      }
    }

    "given versions in different calls" should {
      "be distributive" in forAll(
        genLatestInput,
        Gen.listOf(genSpecifiedVersion),
        Gen.listOf(genSpecifiedVersion),
      ) { (easva, svs1, svs2) =>
        val sv = easva.run._1
        implicit val ev: SubVersion[easva.T] = easva.run._2
        latestWhenAllPresent(latestWhenAllPresent(sv, svs1: _*), svs2: _*) shouldBe latestWhenAllPresent(
          sv,
          svs1 ++ svs2: _*,
        )
      }

      "be symmetric" in forAll(
        genLatestInput,
        Gen.listOf(genSpecifiedVersion),
        Gen.listOf(genSpecifiedVersion),
      ) { (easva, svs1, svs2) =>
        val sv = easva.run._1
        implicit val ev: SubVersion[easva.T] = easva.run._2
        latestWhenAllPresent(latestWhenAllPresent(sv, svs1: _*), svs2: _*) shouldBe latestWhenAllPresent(
          latestWhenAllPresent(sv, svs2: _*),
          svs1: _*,
        )
      }
    }
  }
}

object VersionTimelineSpec {
  import VersionTimeline._

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private val constNil: Any => List[Nothing] = _ => Nil

  private sealed abstract class Exists[F[_]] {
    type T
    val run: F[T]
  }

  private def Exists[T0, F[_]](_run: F[T0]): Exists[F] = {
    final case class ExistsImpl(run: F[T0]) extends Exists[F] {
      type T = T0
      override def productPrefix = "Exists"
    }
    ExistsImpl(_run)
  }

  private val genDefinedLanguageVersion: Gen[LanguageVersion] =
    Gen oneOf inAscendingOrder.foldMap(foldRelease(_)(constNil, constNil, List(_)))

  private final case class Variety[A](gen: Gen[A])(implicit val sv: SubVersion[A])

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private val varieties = NonEmptyList[Variety[_]](
    Variety(Gen oneOf ValueVersions.acceptedVersions),
    Variety(Gen oneOf TransactionVersions.acceptedVersions),
    Variety(genDefinedLanguageVersion),
  )

  private def oneOf[A](xs: NonEmptyList[Gen[A]]): Gen[A] = xs.tail match {
    case ICons(cadr, cddr) => Gen.oneOf(xs.head, cadr, cddr.toList: _*)
    case INil() => xs.head
  }

  type LatestInput[A] = (A, SubVersion[A])
  private val genLatestInput: Gen[Exists[LatestInput]] =
    oneOf(varieties map {
      case vt: Variety[a] => vt.gen map (aa => Exists[a, LatestInput]((aa, vt.sv)))
    })

  private val genSpecifiedVersion: Gen[SpecifiedVersion] =
    oneOf(varieties map { case vt: Variety[a] => vt.gen map vt.sv.inject })

  // remove duplicate in an ordered list
  private def unique[X](l: List[X]) =
    l match {
      case Nil => Nil
      case h :: t => h :: ((l zip t) filterNot { case (x, y) => x == y } map (_._2))
    }
}
