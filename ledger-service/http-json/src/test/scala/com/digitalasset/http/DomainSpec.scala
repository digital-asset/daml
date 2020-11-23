// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import domain._

import org.scalatest._
import scalaz.OneAnd

final class DomainSpec extends FreeSpec with Matchers {
  private val ledgerId = LedgerId("myledger")
  private val appId = ApplicationId("myAppId")
  private val alice = Party("Alice")
  private val bob = Party("Bob")
  "JwtWritePayload" - {
    "parties deduplicates between actAs and readAs" in {
      val payload = JwtWritePayload(ledgerId, appId, actAs = alice, readAs = List(alice, bob))
      payload.parties shouldBe OneAnd(alice, Set(bob))
    }
  }
  "JwtPayload" - {
    "parties deduplicates between actAs and readAs" in {
      val payload = JwtPayload(ledgerId, appId, actAs = List(alice), readAs = List(alice, bob))
      payload.map(_.parties) shouldBe Some(OneAnd(alice, Set(bob)))
    }
    "returns None if readAs and actAs are empty" in {
      val payload = JwtPayload(ledgerId, appId, actAs = List(), readAs = List())
      payload shouldBe None
    }
  }
}
