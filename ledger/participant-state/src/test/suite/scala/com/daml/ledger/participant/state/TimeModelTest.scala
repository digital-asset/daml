// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.time._

import org.scalatest.{Matchers, WordSpec}

class TimeModelTest extends WordSpec with Matchers {

  private val referenceTime = Instant.EPOCH
  private val epsilon = Duration.ofMillis(10L)
  private val timeModel =
    TimeModel(
      minTransactionLatency = Duration.ofSeconds(1L),
      maxClockSkew = Duration.ofSeconds(1L),
      maxTtl = Duration.ofSeconds(30L),
      avgTransactionLatency = Duration.ofSeconds(0L),
      minSkew = Duration.ofSeconds(30L),
      maxSkew = Duration.ofSeconds(30L),
    ).get

  private val referenceMrt = referenceTime.plus(timeModel.maxTtl)

  private def acceptLet(let: Instant): Boolean =
    timeModel.checkLet(referenceTime, let, let.plus(timeModel.maxTtl))

  private def acceptMrt(mrt: Instant): Boolean =
    timeModel.checkLet(referenceTime, mrt.minus(timeModel.maxTtl), mrt)

  private def acceptTtl(mrt: Instant): Boolean = timeModel.checkTtl(referenceTime, mrt)

  "Ledger effective time model checker" when {
    "calculating derived values" should {
      "calculate minTtl correctly" in {
        timeModel.minTtl shouldEqual timeModel.minTransactionLatency.plus(timeModel.maxClockSkew)
      }
    }

    "checking if time is within accepted boundaries" should {
      "succeed if the time equals the current time" in {
        acceptLet(referenceTime) shouldEqual true
      }

      "succeed if the time is higher than the current time and is within tolerance limit" in {
        acceptLet(referenceTime.plus(timeModel.futureAcceptanceWindow).minus(epsilon)) shouldEqual true
      }

      "succeed if the time is equal to the high boundary" in {
        acceptLet(referenceTime.plus(timeModel.futureAcceptanceWindow)) shouldEqual true
      }

      "fail if the time is higher than the high boundary" in {
        acceptLet(referenceTime.plus(timeModel.futureAcceptanceWindow).plus(epsilon)) shouldEqual false
      }

      "fail if the MRT is less than the low boundary" in {
        acceptMrt(referenceMrt.minus(timeModel.maxTtl).minus(epsilon)) shouldEqual false
      }

      "succeed if the MRT is equal to the low boundary" in {
        acceptMrt(referenceMrt.minus(timeModel.maxTtl)) shouldEqual true
      }

      "succeed if the MRT is greater than than the low boundary" in {
        acceptMrt(referenceMrt.minus(timeModel.maxTtl).plus(epsilon)) shouldEqual true
      }
    }
  }

  "TTL time model" when {
    "checking if TTL is within accepted boundaries" should {
      "fail if the TTL is less than than the low boundary" in {
        acceptTtl(referenceTime.plus(timeModel.minTtl).minus(epsilon)) shouldEqual false
      }

      "succeed if the TTL is equal to the low boundary" in {
        acceptTtl(referenceTime.plus(timeModel.minTtl)) shouldEqual true
      }

      "succeed if the TTL is greater than the low boundary" in {
        acceptTtl(referenceTime.plus(timeModel.minTtl).plus(epsilon)) shouldEqual true
      }

      "succeed if the TTL is less than the high boundary" in {
        acceptTtl(referenceTime.plus(timeModel.maxTtl).minus(epsilon)) shouldEqual true
      }

      "succeed if the TTL is equal to the high boundary" in {
        acceptTtl(referenceTime.plus(timeModel.maxTtl)) shouldEqual true
      }

      "fail if the TTL is greater than than the high boundary" in {
        acceptTtl(referenceTime.plus(timeModel.maxTtl).plus(epsilon)) shouldEqual false
      }
    }
  }

  "New ledger time model" when {
    "checking ledger time" should {
      "succeed if the ledger time equals the record time" in {
        timeModel.checkTime(referenceTime, referenceTime).isRight shouldEqual true
      }

      "succeed if the ledger time is higher than the record time and is within tolerance limit" in {
        timeModel.checkTime(referenceTime.plus(epsilon), referenceTime).isRight shouldEqual true
      }

      "succeed if the ledger time is equal to the high boundary" in {
        timeModel
          .checkTime(referenceTime.plus(timeModel.maxSkew), referenceTime)
          .isRight shouldEqual true
      }

      "fail if the ledger time is higher than the high boundary" in {
        timeModel
          .checkTime(referenceTime.plus(timeModel.maxSkew).plus(epsilon), referenceTime)
          .isRight shouldEqual false
      }

      "succeed if the ledger time is lower than the record time and is within tolerance limit" in {
        timeModel.checkTime(referenceTime.minus(epsilon), referenceTime).isRight shouldEqual true
      }

      "succeed if the ledger time is equal to the low boundary" in {
        timeModel
          .checkTime(referenceTime.minus(timeModel.minSkew), referenceTime)
          .isRight shouldEqual true
      }

      "fail if the ledger time is lower than the low boundary" in {
        timeModel
          .checkTime(referenceTime.minus(timeModel.minSkew).minus(epsilon), referenceTime)
          .isRight shouldEqual false
      }
    }
  }
}
