// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.control

import com.daml.ledger.javaapi.data.Party
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BalancerTest extends AnyWordSpec with Matchers {

  "balancer" should {
    lazy val alice = new Party("alice")
    lazy val bob = new Party("bob")
    lazy val charlie = new Party("charlie")
    lazy val david = new Party("david")

    "correctly work with just a single party" in {
      val bal = new Balancer()
      bal.updateMembers(Seq(alice))
      bal.next()
      bal.completed(alice)
      bal.next()
      bal.next()
      bal.completed(alice)
      bal.completed(alice)
    }

    "works correct with several parties" in {
      val bal = new Balancer()
      val members = Seq(alice, bob, charlie, david)
      bal.updateMembers(members)

      val p1 = bal.next()
      val p2 = bal.next()
      Seq(p1) should not contain (p2)
      val p3 = bal.next()
      Seq(p1, p2) should not contain (p3)
      val p4 = bal.next()
      Seq(p1, p2, p3) should not contain (p4)
      bal.completed(p3)
      val p5 = bal.next()
      p5 shouldBe p3
      val p6 = bal.next()
      val p7 = bal.next()
      Seq(p6) should not contain (p7)
      val p8 = bal.next()
      Seq(p6, p7) should not contain (p8)
      Seq(p1, p2, p4, p5, p6, p7).foreach(bal.completed)

      val p10 = bal.next()
      Seq(p8) should not contain (p10)
      val p11 = bal.next()
      Seq(p8, p10) should not contain (p11)
      val p12 = bal.next()
      Seq(p8, p10, p11) should not contain (p12)

    }

  }

}
