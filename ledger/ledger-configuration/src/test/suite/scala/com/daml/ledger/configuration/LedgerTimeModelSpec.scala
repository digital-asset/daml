// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.configuration

import java.time._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LedgerTimeModelSpec extends AnyWordSpec with Matchers {

  private val referenceTime = Instant.EPOCH
  private val epsilon = Duration.ofMillis(10L)
  private val timeModel =
    LedgerTimeModel(
      avgTransactionLatency = Duration.ZERO,
      minSkew = Duration.ofSeconds(30L),
      maxSkew = Duration.ofSeconds(30L),
    ).get
  private val smallSkew = Duration.ofSeconds(1L)
  private val largeSkew = Duration.ofHours(1L)

  "Ledger time model" when {
    "checking ledger time" should {
      "succeed if the ledger time equals the record time" in {
        val result = timeModel.checkTime(referenceTime, referenceTime)

        result should be(Right(()))
      }

      "succeed if the ledger time is higher than the record time and is within tolerance limit" in {
        val result = timeModel.checkTime(referenceTime.plus(epsilon), referenceTime)

        result should be(Right(()))
      }

      "succeed if the ledger time is equal to the high boundary" in {
        val result = timeModel.checkTime(referenceTime.plus(timeModel.maxSkew), referenceTime)

        result should be(Right(()))
      }

      "fail if the ledger time is higher than the high boundary" in {
        val result = timeModel.checkTime(
          referenceTime.plus(timeModel.maxSkew).plus(epsilon),
          referenceTime,
        )

        result should be(
          Left(
            "Ledger time 1970-01-01T00:00:30.010Z outside of range [1969-12-31T23:59:30Z, 1970-01-01T00:00:30Z]"
          )
        )
      }

      "succeed if the ledger time is lower than the record time and is within tolerance limit" in {
        val result = timeModel.checkTime(referenceTime.minus(epsilon), referenceTime)

        result should be(Right(()))
      }

      "succeed if the ledger time is equal to the low boundary" in {
        val result = timeModel.checkTime(referenceTime.minus(timeModel.minSkew), referenceTime)

        result should be(Right(()))
      }

      "fail if the ledger time is lower than the low boundary" in {
        val result = timeModel.checkTime(
          referenceTime.minus(timeModel.minSkew).minus(epsilon),
          referenceTime,
        )

        result should be(
          Left(
            "Ledger time 1969-12-31T23:59:29.990Z outside of range [1969-12-31T23:59:30Z, 1970-01-01T00:00:30Z]"
          )
        )
      }

      "succeed if the ledger time is equal to the high boundary (asymmetric case)" in {
        val instance = createAsymmetricTimeModel(largeSkew, smallSkew)

        val result = instance.checkTime(referenceTime.plus(instance.maxSkew), referenceTime)

        result should be(Right(()))
      }

      "succeed if the ledger time is equal to the low boundary (asymmetric case)" in {
        val instance = createAsymmetricTimeModel(smallSkew, largeSkew)

        val result = instance.checkTime(referenceTime.minus(instance.minSkew), referenceTime)

        result should be(Right(()))
      }

      "fail if the ledger time is higher than the high boundary (asymmetric case)" in {
        val instance = createAsymmetricTimeModel(largeSkew, smallSkew)

        val result =
          instance.checkTime(referenceTime.plus(instance.maxSkew).plus(epsilon), referenceTime)

        result should be(
          Left(
            "Ledger time 1970-01-01T00:00:01.010Z outside of range [1969-12-31T23:00:00Z, 1970-01-01T00:00:01Z]"
          )
        )
      }

      "fail if the ledger time is lower than the low boundary (asymmetric case)" in {
        val instance = createAsymmetricTimeModel(smallSkew, largeSkew)

        val result = instance
          .checkTime(referenceTime.minus(instance.minSkew).minus(epsilon), referenceTime)

        result should be(
          Left(
            "Ledger time 1969-12-31T23:59:58.990Z outside of range [1969-12-31T23:59:59Z, 1970-01-01T01:00:00Z]"
          )
        )
      }

      "produce a valid error message" in {
        val timeModel = LedgerTimeModel(
          avgTransactionLatency = Duration.ZERO,
          minSkew = Duration.ofSeconds(10L),
          maxSkew = Duration.ofSeconds(20L),
        ).get
        val ledgerTime = "2000-01-01T12:00:00Z"
        val recordTime = "2000-01-01T12:30:00Z"
        val lowerBound = "2000-01-01T12:29:50Z"
        val upperBound = "2000-01-01T12:30:20Z"
        val result = timeModel
          .checkTime(Instant.parse(ledgerTime), Instant.parse(recordTime))

        result should be(
          Left(s"Ledger time $ledgerTime outside of range [$lowerBound, $upperBound]")
        )
      }
    }
  }

  private def createAsymmetricTimeModel(minSkew: Duration, maxSkew: Duration): LedgerTimeModel =
    LedgerTimeModel(
      avgTransactionLatency = Duration.ZERO,
      minSkew = minSkew,
      maxSkew = maxSkew,
    ).get
}
