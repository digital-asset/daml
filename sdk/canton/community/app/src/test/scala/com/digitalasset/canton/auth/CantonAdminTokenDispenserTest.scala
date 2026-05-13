// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.clock.AdjustableClock
import com.digitalasset.canton.crypto.RandomOps
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Duration, ZoneId}

class CantonAdminTokenDispenserTest extends AnyWordSpec with Matchers {

  // Simple deterministic RandomOps for testing
  object TestRandomOps extends RandomOps {
    private var counter = 0
    override def generateRandomBytes(length: Int): Array[Byte] = {
      counter += 1
      Array.fill(length)((counter % 256).toByte)
    }
  }

  private val testNow = Clock.systemUTC().instant()

  "CantonAdminTokenDispenser" should {
    "generate and rotate tokens correctly" in {
      val clock = AdjustableClock(
        baseClock = Clock.fixed(testNow, ZoneId.of("UTC")),
        offset = Duration.ZERO,
      )
      val tokenDuration = Duration.ofMinutes(10)
      val rotationInterval = tokenDuration.dividedBy(2)
      val dispenser = new CantonAdminTokenDispenser(TestRandomOps, tokenDuration, None, clock)

      val token1 = dispenser.getCurrentToken
      token1.secret.length should be > 0

      // Simulate time passing less than rotation interval
      clock.fastForward(rotationInterval)
      dispenser.getCurrentToken.secret shouldEqual token1.secret

      // Simulate time passing beyond rotation interval
      clock.fastForward(Duration.ofSeconds(1))
      dispenser.getCurrentToken.secret should not equal token1.secret
    }

    "validate current and previous tokens within their validity window" in {
      val clock = AdjustableClock(
        baseClock = Clock.fixed(testNow, ZoneId.of("UTC")),
        offset = Duration.ZERO,
      )
      val tokenDuration = Duration.ofMinutes(10)
      val rotationInterval = tokenDuration.dividedBy(2)
      val dispenser = new CantonAdminTokenDispenser(TestRandomOps, tokenDuration, None, clock)

      val token1 = dispenser.getCurrentToken
      // Current token should be valid
      dispenser.checkToken(token1.secret) shouldBe true

      // Simulate rotation
      clock.fastForward(rotationInterval.plusSeconds(1))
      val token2 = dispenser.getCurrentToken

      // Previous token should still be valid
      dispenser.checkToken(token1.secret) shouldBe true
      // New token should be valid
      dispenser.checkToken(token2.secret) shouldBe true

      // Simulate expiry of first token
      clock.fastForward(rotationInterval)
      dispenser.checkToken(token1.secret) shouldBe false
      // Previous token should still be valid
      dispenser.checkToken(token2.secret) shouldBe true

      // Simulate expiry of second token
      clock.fastForward(rotationInterval)
      dispenser.checkToken(token1.secret) shouldBe false
      dispenser.checkToken(token2.secret) shouldBe false

      val token3 = dispenser.getCurrentToken
      dispenser.checkToken(token3.secret) shouldBe true
    }

    "fixed token is valid independent of the time" in {
      val tokenDuration = Duration.ofMinutes(10)
      val fixedToken = Some("fixed-secret")
      val clock = AdjustableClock(
        baseClock = Clock.fixed(testNow, ZoneId.of("UTC")),
        offset = Duration.ZERO,
      )
      val dispenser = new CantonAdminTokenDispenser(TestRandomOps, tokenDuration, fixedToken, clock)

      dispenser.checkToken("fixed-secret") shouldBe true

      // Simulate time passing far beyond token duration
      clock.fastForward(tokenDuration.multipliedBy(10))
      dispenser.checkToken("fixed-secret") shouldBe true
    }
  }
}
