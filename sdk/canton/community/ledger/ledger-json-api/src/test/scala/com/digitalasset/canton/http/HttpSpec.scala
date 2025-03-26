// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.nonempty.NonEmpty
import com.daml.scalatest.FreeSpecCheckLaws
import com.digitalasset.canton.http.{DisclosedContract, JwtPayload, JwtWritePayload}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import scalaz.NonEmptyList

final class HttpSpec extends AnyFreeSpec with Matchers with FreeSpecCheckLaws {
  import HttpSpec.*

  private val userId = UserId("myUserId")
  private val alice = Party("Alice")
  private val bob = Party("Bob")
  "JwtWritePayload" - {
    "parties deduplicates between actAs/submitter and readAs" in {
      val payload =
        JwtWritePayload(userId, submitter = NonEmptyList(alice), readAs = List(alice, bob))
      payload.parties should ===(NonEmpty(Set, alice, bob))
    }
  }
  "JwtPayload" - {
    "parties deduplicates between actAs and readAs" in {
      val payload = JwtPayload(userId, actAs = List(alice), readAs = List(alice, bob))
      payload.map(_.parties) should ===(Some(NonEmpty(Set, alice, bob)))
    }
    "returns None if readAs and actAs are empty" in {
      val payload = JwtPayload(userId, actAs = List(), readAs = List())
      payload shouldBe None
    }
  }

  "DisclosedContract" - {
    import json.JsonProtocolTest.*
    import scalaz.scalacheck.ScalazProperties as SZP

    "bitraverse" - {
      checkLaws(SZP.traverse.laws[DisclosedContract])
    }
  }
}

object HttpSpec {
  import scalaz.Equal

  implicit val eqDisclosedContract: Equal[DisclosedContract[Int]] =
    Equal.equalA
}
