// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.daml.lf.data.FlatSpecCheckLaws
import ContractsFetch.InsertDeleteStep

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import scalaz.{Equal, Monoid}
import scalaz.syntax.semigroup._
import scalaz.scalacheck.ScalazProperties

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class ContractsFetchTest
    extends FlatSpec
    with Matchers
    with FlatSpecCheckLaws
    with GeneratorDrivenPropertyChecks {
  import ContractsFetchTest._

  behavior of "InsertDeleteStep append monoid"

  checkLaws(ScalazProperties.monoid.laws[IDS])

  behavior of "InsertDeleteStep.appendWithCid"

  it should "never insert a deleted item" in forAll { (x: IDS, y: IDS) =>
    val xy = x |+| y.copy(inserts = y.inserts filterNot x.deletes)
    xy.inserts.toSet intersect xy.deletes shouldBe empty
  }

  it should "preserve every left delete" in forAll { (x: IDS, y: IDS) =>
    val xy = x |+| y
    xy.deletes should contain allElementsOf x.deletes
  }

  it should "preserve at least right deletes absent in left inserts" in forAll { (x: IDS, y: IDS) =>
    val xy = x |+| y
    // xy.deletes _may_ contain x.inserts; it is semantically irrelevant
    xy.deletes should contain allElementsOf (y.deletes -- x.inserts)
  }

  it should "preserve append absent deletes" in forAll { (x: Vector[Cid], y: Vector[Cid]) =>
    val xy = InsertDeleteStep(x, Set.empty) |+| InsertDeleteStep(y, Set.empty)
    xy.inserts should ===(x ++ y)
  }
}

object ContractsFetchTest {
  import org.scalacheck.{Arbitrary, Shrink}
  import Arbitrary.arbitrary

  type IDS = InsertDeleteStep[Cid]
  type Cid = String

  private implicit val `IDS monoid`
    : Monoid[IDS] = Monoid instance (_.appendWithCid(_)(identity), InsertDeleteStep(
    Vector.empty,
    Set.empty,
  ))

  implicit val `IDS arb`: Arbitrary[IDS] =
    Arbitrary(arbitrary[(Vector[Cid], Set[Cid])] map {
      case (is, ds) =>
        InsertDeleteStep(is filterNot ds, ds)
    })

  implicit val `IDS shr`: Shrink[IDS] =
    Shrink.xmap[(Vector[Cid], Set[Cid]), IDS](
      (InsertDeleteStep[Cid] _).tupled,
      step => (step.inserts, step.deletes),
    )

  implicit val `IDS eq`: Equal[IDS] = Equal.equalA
}
