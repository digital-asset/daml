// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import domain._
import com.daml.scalautil.nonempty.NonEmpty
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import scalaz.NonEmptyList

import scala.collection.compat._

final class DomainSpec extends AnyFreeSpec with Matchers {
  private val ledgerId = LedgerId("myledger")
  private val appId = ApplicationId("myAppId")
  private val alice = Party("Alice")
  private val bob = Party("Bob")
  "JwtWritePayload" - {
    "parties deduplicates between actAs/submitter and readAs" in {
      val payload =
        JwtWritePayload(ledgerId, appId, submitter = NonEmptyList(alice), readAs = List(alice, bob))
      payload.parties should ===(NonEmpty.pour(alice, bob) into Set)
    }
  }
  "JwtPayload" - {
    "parties deduplicates between actAs and readAs" in {
      val payload = JwtPayload(ledgerId, appId, actAs = List(alice), readAs = List(alice, bob))
      payload.map(_.parties) should ===(Some(NonEmpty.pour(alice, bob) into Set))
    }
    "returns None if readAs and actAs are empty" in {
      val payload = JwtPayload(ledgerId, appId, actAs = List(), readAs = List())
      payload shouldBe None
    }
  }
}
