// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.configuration

import org.scalatest.{Matchers, WordSpec}

class LedgerIdRequirementTest extends WordSpec with Matchers {

  "LedgerIdRequirement" should {

    "allow verify ledger" in {
      val expected = "ledger-a"
      val underTest = LedgerIdRequirement(expected, enabled = true)
      underTest.isAccepted(expected) shouldBe true
      underTest.isAccepted("not-a") shouldBe false
    }

    "empty constructor should allow any ledger" in {
      LedgerIdRequirement("", enabled = false).isAccepted("any-ledger") shouldBe true
    }

    "allow allow deprecated construction" in {
      val expected = "ledger-b"
      val v1 = LedgerIdRequirement(expected, enabled = true)
      val v2 = LedgerIdRequirement(Some(expected))
      v1 shouldBe v2
    }

  }
}

