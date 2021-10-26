// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger

import com.daml.ledger.configuration.LedgerTimeModel
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Time.Timestamp
import com.google.protobuf.any.{Any => AnyProto}
import com.google.rpc.error_details.ErrorInfo
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RejectionSpec extends AnyWordSpec with Matchers {
  "Rejection.NoLedgerConfiguration" should {
    "convert to a state rejection reason" in {
      Rejection.NoLedgerConfiguration.toStateRejectionReason should be(
        state.Update.CommandRejected.FinalReason(
          StatusProto.of(
            code = Status.Code.ABORTED.value,
            message = "No ledger configuration available, cannot validate ledger time",
            details = Seq(
              AnyProto.pack(
                ErrorInfo.of(
                  reason = "NO_LEDGER_CONFIGURATION",
                  domain = "com.daml.on.sql",
                  metadata = Map.empty,
                )
              )
            ),
          )
        )
      )
    }
  }

  "Rejection.InvalidLedgerTime" should {
    "convert to a state rejection reason" in {
      val ledgerTime = Timestamp.assertFromString("2021-07-20T09:30:00Z")
      val lowerBound = Timestamp.assertFromString("2021-07-20T09:00:00Z")
      val upperBound = Timestamp.assertFromString("2021-07-20T09:10:00Z")
      val outOfRange = LedgerTimeModel.OutOfRange(ledgerTime, lowerBound, upperBound)

      Rejection.InvalidLedgerTime(outOfRange).toStateRejectionReason should be(
        state.Update.CommandRejected.FinalReason(
          StatusProto.of(
            code = Status.Code.ABORTED.value,
            message = outOfRange.message,
            details = Seq(
              AnyProto.pack(
                ErrorInfo.of(
                  reason = "INVALID_LEDGER_TIME",
                  domain = "com.daml.on.sql",
                  metadata = Map(
                    "ledgerTime" -> ledgerTime.toString,
                    "lowerBound" -> lowerBound.toString,
                    "upperBound" -> upperBound.toString,
                  ),
                )
              )
            ),
          )
        )
      )
    }
  }
}
