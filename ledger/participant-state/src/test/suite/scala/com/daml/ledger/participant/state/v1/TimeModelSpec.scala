// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.time._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TimeModelSpec extends AnyWordSpec with Matchers {

  private val referenceTime = Instant.EPOCH
  private val epsilon = Duration.ofMillis(10L)
  private val timeModel =
    TimeModel(
      avgTransactionLatency = Duration.ZERO,
      minSkew = Duration.ofSeconds(30L),
      maxSkew = Duration.ofSeconds(30L),
    ).get
  private val smallSkew = Duration.ofSeconds(1L)
  private val largeSkew = Duration.ofHours(1L)

  "Ledger time model" when {
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

      "succeed if the ledger time is equal to the high boundary (asymmetric case)" in {
        val instance = createAsymmetricTimeModel(largeSkew, smallSkew)

        instance
          .checkTime(referenceTime.plus(instance.maxSkew), referenceTime)
          .isRight shouldEqual true
      }

      "succeed if the ledger time is equal to the low boundary (asymmetric case)" in {
        val instance = createAsymmetricTimeModel(smallSkew, largeSkew)

        instance
          .checkTime(referenceTime.minus(instance.minSkew), referenceTime)
          .isRight shouldEqual true
      }

      "fail if the ledger time is higher than the high boundary (asymmetric case)" in {
        val instance = createAsymmetricTimeModel(largeSkew, smallSkew)

        instance
          .checkTime(referenceTime.plus(instance.maxSkew).plus(epsilon), referenceTime)
          .isLeft shouldEqual true
      }

      "fail if the ledger time is lower than the low boundary (asymmetric case)" in {
        val instance = createAsymmetricTimeModel(smallSkew, largeSkew)

        instance
          .checkTime(referenceTime.minus(instance.minSkew).minus(epsilon), referenceTime)
          .isLeft shouldEqual true
      }

      "produce a valid error message" in {
        val timeModel =
          TimeModel(
            avgTransactionLatency = Duration.ZERO,
            minSkew = Duration.ofSeconds(10L),
            maxSkew = Duration.ofSeconds(20L),
          ).get
        val ledgerTime = "2000-01-01T12:00:00Z"
        val recordTime = "2000-01-01T12:30:00Z"
        val lowerBound = "2000-01-01T12:29:50Z"
        val upperBound = "2000-01-01T12:30:20Z"
        val expectedMessage = s"Ledger time $ledgerTime outside of range [$lowerBound, $upperBound]"

        timeModel
          .checkTime(Instant.parse(ledgerTime), Instant.parse(recordTime)) shouldEqual Left(
          expectedMessage)
      }
    }
  }

  private def createAsymmetricTimeModel(minSkew: Duration, maxSkew: Duration): TimeModel =
    TimeModel(
      avgTransactionLatency = Duration.ZERO,
      minSkew = minSkew,
      maxSkew = maxSkew,
    ).get
}
