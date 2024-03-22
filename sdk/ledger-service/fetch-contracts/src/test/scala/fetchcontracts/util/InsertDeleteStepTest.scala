// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.fetchcontracts.util

import com.daml.scalatest.FlatSpecCheckLaws

import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalaz.scalacheck.ScalazProperties
import scalaz.syntax.semigroup._
import scalaz.{@@, Equal, Tag}

class InsertDeleteStepTest
    extends AnyFlatSpec
    with Matchers
    with FlatSpecCheckLaws
    with ScalaCheckDrivenPropertyChecks {
  import InsertDeleteStepTest._

  override implicit val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  behavior of "append monoid"

  checkLaws(ScalazProperties.monoid.laws[IDS])

  behavior of "append"

  it should "never insert a deleted item" in forAll { (x: IDS, y: IDS) =>
    val xy = x |+| y.copy(inserts = y.inserts filterNot Cid.subst(x.deletes.keySet))
    xy.inserts.toSet intersect Cid.subst(xy.deletes.keySet) shouldBe empty
  }

  it should "preserve every left delete" in forAll { (x: IDS, y: IDS) =>
    val xy = x |+| y
    xy.deletes.keySet should contain allElementsOf x.deletes.keySet
  }

  it should "preserve at least right deletes absent in left inserts" in forAll { (x: IDS, y: IDS) =>
    val xy = x |+| y
    // xy.deletes _may_ contain x.inserts; it is semantically irrelevant
    xy.deletes.keySet should contain allElementsOf (y.deletes.keySet -- Cid.unsubst(x.inserts))
  }

  it should "preserve append absent deletes" in forAll { (x: Vector[Cid], y: Vector[Cid]) =>
    val xy = InsertDeleteStep(x, Map.empty[String, Unit]) |+| InsertDeleteStep(y, Map.empty)
    xy.inserts should ===(x ++ y)
  }
}

object InsertDeleteStepTest {
  import org.scalacheck.{Arbitrary, Gen, Shrink}
  import Arbitrary.arbitrary

  type IDS = InsertDeleteStep[Unit, Cid]
  sealed trait Alpha
  type Cid = String @@ Alpha
  val Cid = Tag.of[Alpha]

  implicit val `Alpha arb`: Arbitrary[Cid] =
    Cid subst Arbitrary(Gen.alphaUpperChar map (_.toString))

  private[util] implicit val `test Cid`: InsertDeleteStep.Cid[Cid] = Cid.unwrap(_)

  implicit val `IDS arb`: Arbitrary[IDS] =
    Arbitrary(arbitrary[(Vector[Cid], Map[Cid, Unit])] map { case (is, ds) =>
      InsertDeleteStep(is filterNot ds.keySet, Cid.unsubst[Map[*, Unit], String](ds))
    })

  implicit val `IDS shr`: Shrink[IDS] =
    Shrink.xmap[(Vector[Cid], Map[Cid, Unit]), IDS](
      { case (is, ds) => InsertDeleteStep(is, Cid.unsubst[Map[*, Unit], String](ds)) },
      step => (step.inserts, Cid.subst[Map[*, Unit], String](step.deletes)),
    )

  implicit val `IDS eq`: Equal[IDS] = Equal.equalA
}
