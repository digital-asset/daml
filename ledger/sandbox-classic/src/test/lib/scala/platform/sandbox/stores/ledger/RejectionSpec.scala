// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger

import com.daml.error.{DamlContextualizedErrorLogger, ErrorCodesVersionSwitcher}
import com.daml.ledger.configuration.LedgerTimeModel
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.server.api.validation.ErrorFactories
import com.google.protobuf.any.{Any => AnyProto}
import com.google.rpc.error_details.{ErrorInfo, RequestInfo}
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RejectionSpec extends AnyWordSpec with Matchers {
  private val submissionId = "12345678"
  private implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(
      ContextualizedLogger.get(getClass),
      LoggingContext.ForTesting,
      Some(submissionId),
    )

  private val errorFactoriesV1 = ErrorFactories(new ErrorCodesVersionSwitcher(false))
  private val errorFactoriesV2 = ErrorFactories(new ErrorCodesVersionSwitcher(true))

  "Rejection.NoLedgerConfiguration" should {
    "convert to a state rejection reason (V1 errors)" in {
      Rejection.NoLedgerConfiguration.toStateRejectionReason(errorFactoriesV1) should be(
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

    "convert to a state rejection reason (V2 errors)" in {
      val actualRejectionReason =
        Rejection.NoLedgerConfiguration.toStateRejectionReason(errorFactoriesV2)
      actualRejectionReason.code shouldBe Status.Code.NOT_FOUND.value()
      actualRejectionReason.message shouldBe "LEDGER_CONFIGURATION_NOT_FOUND(11,12345678): The ledger configuration could not be retrieved: Cannot validate ledger time."

      val (errorInfo, requestInfo) = extractDetails(actualRejectionReason.status.details)
      errorInfo shouldBe com.google.rpc.error_details.ErrorInfo(
        reason = "LEDGER_CONFIGURATION_NOT_FOUND",
        metadata = Map(
          "message" -> "Cannot validate ledger time",
          "category" -> "11",
          "definite_answer" -> "false",
        ),
      )

      requestInfo shouldBe RequestInfo(requestId = submissionId)
    }
  }

  "Rejection.InvalidLedgerTime" should {
    val ledgerTime = Timestamp.assertFromString("2021-07-20T09:30:00Z")
    val lowerBound = Timestamp.assertFromString("2021-07-20T09:00:00Z")
    val upperBound = Timestamp.assertFromString("2021-07-20T09:10:00Z")
    val outOfRange = LedgerTimeModel.OutOfRange(ledgerTime, lowerBound, upperBound)

    "convert to a state rejection reason (V1 errors)" in {
      Rejection.InvalidLedgerTime(outOfRange).toStateRejectionReason(errorFactoriesV1) should be(
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

    "convert to a state rejection reason (V2 errors)" in {
      val actualRejectionReason =
        Rejection.InvalidLedgerTime(outOfRange).toStateRejectionReason(errorFactoriesV2)

      actualRejectionReason.code shouldBe Status.Code.FAILED_PRECONDITION.value()
      actualRejectionReason.message shouldBe "INVALID_LEDGER_TIME(9,12345678): Invalid ledger time: Ledger time 2021-07-20T09:30:00Z outside of range [2021-07-20T09:00:00Z, 2021-07-20T09:10:00Z]"

      val (errorInfo, requestInfo) = extractDetails(actualRejectionReason.status.details)

      errorInfo shouldBe com.google.rpc.error_details.ErrorInfo(
        reason = "INVALID_LEDGER_TIME",
        metadata = Map(
          "ledger_time" -> "2021-07-20T09:30:00Z",
          "ledger_time_lower_bound" -> "2021-07-20T09:00:00Z",
          "ledger_time_upper_bound" -> "2021-07-20T09:10:00Z",
          "category" -> "9",
          "message" -> "Ledger time 2021-07-20T09:30:00Z outside of range [2021-07-20T09:00:00Z, 2021-07-20T09:10:00Z]",
          "definite_answer" -> "false",
        ),
      )

      requestInfo shouldBe RequestInfo(requestId = submissionId)
    }
  }

  def extractDetails(details: Seq[AnyProto]): (ErrorInfo, RequestInfo) = {
    details should have size 2L
    details match {
      case d1 :: d2 :: Nil if d1.is(ErrorInfo) && d2.is[RequestInfo] =>
        d1.unpack[ErrorInfo] -> d2.unpack[RequestInfo]
      case d1 :: d2 :: Nil if d2.is(ErrorInfo) && d1.is[RequestInfo] =>
        d2.unpack[ErrorInfo] -> d1.unpack[RequestInfo]
      case details => fail(s"Unexpected details: $details")
    }
  }
}
