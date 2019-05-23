// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.time

import java.time._

import com.digitalasset.platform.services.time.{TimeModel, TimeModelChecker}
import org.scalatest.{Matchers, WordSpec}

class TimeModelTest extends WordSpec with Matchers {

  private val referenceTime = Instant.EPOCH
  private val epsilon = Duration.ofMillis(10L)
  private val sut =
    TimeModel(Duration.ofSeconds(1L), Duration.ofSeconds(1L), Duration.ofSeconds(30L)).get

  private val checker = TimeModelChecker(sut)

  private val referenceMrt = referenceTime.plus(sut.maxTtl)

  private def acceptLet(let: Instant): Boolean =
    checker.checkLet(referenceTime, let, let.plus(sut.maxTtl))

  private def acceptMrt(mrt: Instant): Boolean =
    checker.checkLet(referenceTime, mrt.minus(sut.maxTtl), mrt)

  private def acceptTtl(mrt: Instant): Boolean = checker.checkTtl(referenceTime, mrt)

  "Ledger effective time model checker" when {
    "calculating derived values" should {
      "calculate minTtl correctly" in {
        sut.minTtl shouldEqual sut.minTransactionLatency.plus(sut.maxClockSkew)
      }
    }

    "checking if time is within accepted boundaries" should {
      "succeed if the time equals the current time" in {
        acceptLet(referenceTime) shouldEqual true
      }

      "succeed if the time is higher than the current time and is within tolerance limit" in {
        acceptLet(referenceTime.plus(sut.futureAcceptanceWindow).minus(epsilon)) shouldEqual true
      }

      "succeed if the time is equal to the high boundary" in {
        acceptLet(referenceTime.plus(sut.futureAcceptanceWindow)) shouldEqual true
      }

      "fail if the time is higher than the high boundary" in {
        acceptLet(referenceTime.plus(sut.futureAcceptanceWindow).plus(epsilon)) shouldEqual false
      }

      "fail if the MRT is less than the low boundary" in {
        acceptMrt(referenceMrt.minus(sut.maxTtl).minus(epsilon)) shouldEqual false
      }

      "succeed if the MRT is equal to the low boundary" in {
        acceptMrt(referenceMrt.minus(sut.maxTtl)) shouldEqual true
      }

      "succeed if the MRT is greater than than the low boundary" in {
        acceptMrt(referenceMrt.minus(sut.maxTtl).plus(epsilon)) shouldEqual true
      }
    }
  }

  "TTL time model checker" when {
    "checking if TTL is within accepted boundaries" should {
      "fail if the TTL is less than than the low boundary" in {
        acceptTtl(referenceTime.plus(sut.minTtl).minus(epsilon)) shouldEqual false
      }

      "succeed if the TTL is equal to the low boundary" in {
        acceptTtl(referenceTime.plus(sut.minTtl)) shouldEqual true
      }

      "succeed if the TTL is greater than the low boundary" in {
        acceptTtl(referenceTime.plus(sut.minTtl).plus(epsilon)) shouldEqual true
      }

      "succeed if the TTL is less than the high boundary" in {
        acceptTtl(referenceTime.plus(sut.maxTtl).minus(epsilon)) shouldEqual true
      }

      "succeed if the TTL is equal to the high boundary" in {
        acceptTtl(referenceTime.plus(sut.maxTtl)) shouldEqual true
      }

      "fail if the TTL is greater than than the high boundary" in {
        acceptTtl(referenceTime.plus(sut.maxTtl).plus(epsilon)) shouldEqual false
      }
    }
  }
}
