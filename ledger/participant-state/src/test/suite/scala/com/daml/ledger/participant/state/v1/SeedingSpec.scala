// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SeedingSpec extends AnyWordSpec with Matchers {
  "StaticRandomSeedService" should {
    "return the same sequence of random numbers across multiple runs" in {
      val gen1 = new SeedService.StaticRandom("one key")
      val gen2 = new SeedService.StaticRandom("one key")

      val hashes1 = List.fill(100)(gen1.nextSeed())
      val hashes2 = List.fill(100)(gen2.nextSeed())
      hashes1 shouldEqual hashes2
    }
  }
}
