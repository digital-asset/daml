// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.configuration

import com.daml.lf.data.Time.Timestamp
import com.digitalasset.canton.ledger.configuration.LedgerTimeModel.OutOfRange
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.*

class LedgerTimeModelSpec extends AnyWordSpec with Matchers {

  private val referenceTime = Timestamp.Epoch
  private val epsilon = Duration.ofMillis(10L)
  private val defaultSkew = Duration.ofSeconds(30L)
  private val timeModel =
    LedgerTimeModel(
      avgTransactionLatency = Duration.ZERO,
      minSkew = defaultSkew,
      maxSkew = defaultSkew,
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
        val result = timeModel.checkTime(referenceTime.add(epsilon), referenceTime)

        result should be(Right(()))
      }

      "succeed if the ledger time is equal to the high boundary" in {
        val result = timeModel.checkTime(referenceTime.add(timeModel.maxSkew), referenceTime)

        result should be(Right(()))
      }

      "fail if the ledger time is higher than the high boundary" in {
        val ledgerTime = referenceTime.add(timeModel.maxSkew).add(epsilon)
        val minRecordTime = referenceTime.subtract(defaultSkew)
        val maxRecordTime = referenceTime.add(defaultSkew)

        val result = timeModel.checkTime(ledgerTime, referenceTime)

        result should be(Left(OutOfRange(ledgerTime, minRecordTime, maxRecordTime)))
      }

      "succeed if the ledger time is lower than the record time and is within tolerance limit" in {
        val result = timeModel.checkTime(referenceTime.subtract(epsilon), referenceTime)

        result should be(Right(()))
      }

      "succeed if the ledger time is equal to the low boundary" in {
        val result = timeModel.checkTime(referenceTime.subtract(timeModel.minSkew), referenceTime)

        result should be(Right(()))
      }

      "fail if the ledger time is lower than the low boundary" in {
        val ledgerTime = referenceTime.subtract(timeModel.minSkew).subtract(epsilon)
        val minRecordTime = referenceTime.subtract(defaultSkew)
        val maxRecordTime = referenceTime.add(defaultSkew)

        val result = timeModel.checkTime(ledgerTime, referenceTime)

        result should be(Left(OutOfRange(ledgerTime, minRecordTime, maxRecordTime)))
      }

      "succeed if the ledger time is equal to the high boundary (asymmetric case)" in {
        val instance = createAsymmetricTimeModel(minSkew = largeSkew, maxSkew = smallSkew)

        val result = instance.checkTime(referenceTime.add(instance.maxSkew), referenceTime)

        result should be(Right(()))
      }

      "succeed if the ledger time is equal to the low boundary (asymmetric case)" in {
        val instance = createAsymmetricTimeModel(minSkew = smallSkew, maxSkew = largeSkew)

        val result = instance.checkTime(referenceTime.subtract(instance.minSkew), referenceTime)

        result should be(Right(()))
      }

      "fail if the ledger time is higher than the high boundary (asymmetric case)" in {
        val instance = createAsymmetricTimeModel(minSkew = largeSkew, maxSkew = smallSkew)

        val ledgerTime = referenceTime.add(instance.maxSkew).add(epsilon)
        val minRecordTime = referenceTime.subtract(largeSkew)
        val maxRecordTime = referenceTime.add(smallSkew)

        val result = instance.checkTime(ledgerTime, referenceTime)

        result should be(Left(OutOfRange(ledgerTime, minRecordTime, maxRecordTime)))
      }

      "fail if the ledger time is lower than the low boundary (asymmetric case)" in {
        val instance = createAsymmetricTimeModel(minSkew = smallSkew, maxSkew = largeSkew)

        val ledgerTime = referenceTime.subtract(instance.minSkew).subtract(epsilon)
        val minRecordTime = referenceTime.subtract(smallSkew)
        val maxRecordTime = referenceTime.add(largeSkew)

        val result = instance.checkTime(ledgerTime, referenceTime)

        result should be(Left(OutOfRange(ledgerTime, minRecordTime, maxRecordTime)))
      }

      "produce a valid error message" in {
        val timeModel = LedgerTimeModel(
          avgTransactionLatency = Duration.ZERO,
          minSkew = Duration.ofSeconds(10L),
          maxSkew = Duration.ofSeconds(20L),
        ).get

        val ledgerTime = Timestamp.assertFromInstant(Instant.parse("2000-01-01T12:00:00Z"))
        val recordTime = Timestamp.assertFromInstant(Instant.parse("2000-01-01T12:30:00Z"))
        val minRecordTime = Timestamp.assertFromInstant(Instant.parse("2000-01-01T12:29:50Z"))
        val maxRecordTime = Timestamp.assertFromInstant(Instant.parse("2000-01-01T12:30:20Z"))

        val result = timeModel.checkTime(ledgerTime, recordTime)

        result should be(Left(OutOfRange(ledgerTime, minRecordTime, maxRecordTime)))
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
