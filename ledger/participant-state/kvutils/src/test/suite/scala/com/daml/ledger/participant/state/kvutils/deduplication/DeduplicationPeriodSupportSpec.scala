// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.deduplication

import java.time.{Duration, Instant}

import com.daml.error.ErrorsAssertions
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.TestLoggers
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.configuration.LedgerTimeModel
import com.daml.ledger.offset.Offset
import com.daml.lf.crypto.Hash
import com.daml.lf.data.{Ref, Time}
import io.grpc.{Status, StatusRuntimeException}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class DeduplicationPeriodSupportSpec
    extends AsyncWordSpec
    with MockitoSugar
    with Matchers
    with AkkaBeforeAndAfterAll
    with TestLoggers
    with ArgumentMatchersSugar
    with ErrorsAssertions {

  "using deduplication duration" should {
    "validate and return success" in {
      val fixture = getFixture
      import fixture._
      callServiceWithDeduplicationPeriod(durationPeriod)
        .map { result =>
          result shouldBe durationPeriod
        }
    }

    "validate and return failure" in {
      val fixture = getFixture
      import fixture._
      recoverToExceptionIf[StatusRuntimeException](
        callServiceWithDeduplicationPeriod(durationPeriodGreaterThanMax)
      )
        .map { result =>
          assertMatchesErrorCode(
            actual = result,
            expectedErrorCode = LedgerApiErrors.RequestValidation.InvalidDeduplicationPeriodField,
          )
        }
    }
  }

  "using deduplication offset" should {
    "convert offset and validate result" in {
      val fixture = getFixture
      import fixture._
      val offsetPeriod = DeduplicationPeriod.DeduplicationOffset(
        deduplicationPeriodOffset
      )
      when(
        periodConverter.convertOffsetToDuration(
          offset,
          applicationId,
          Set.empty,
          maxRecordTimeFromSubmissionTime,
        )
      ).thenReturn(Future.successful(Right(durationPeriod.duration)))
      callServiceWithDeduplicationPeriod(offsetPeriod)
        .map { result =>
          verify(periodConverter).convertOffsetToDuration(
            offset,
            applicationId,
            Set.empty,
            maxRecordTimeFromSubmissionTime,
          )
          result shouldBe durationPeriod
        }
    }

    "convert offset and return validation failure" in {
      val fixture = getFixture
      import fixture._
      val offsetPeriod = DeduplicationPeriod.DeduplicationOffset(
        deduplicationPeriodOffset
      )
      when(
        periodConverter.convertOffsetToDuration(
          offset,
          applicationId,
          Set.empty,
          maxRecordTimeFromSubmissionTime,
        )
      ).thenReturn(Future.successful(Right(durationPeriodGreaterThanMax.duration)))
      recoverToExceptionIf[StatusRuntimeException](callServiceWithDeduplicationPeriod(offsetPeriod))
        .map { result =>
          verify(periodConverter).convertOffsetToDuration(
            offset,
            applicationId,
            Set.empty,
            maxRecordTimeFromSubmissionTime,
          )
          assertMatchesErrorCode(
            actual = result,
            expectedErrorCode = LedgerApiErrors.RequestValidation.InvalidDeduplicationPeriodField,
          )
        }
    }

    "fail to convert offset and return failure" in {
      val fixture = getFixture
      import fixture._
      val offsetPeriod = DeduplicationPeriod.DeduplicationOffset(
        deduplicationPeriodOffset
      )
      when(
        periodConverter.convertOffsetToDuration(
          offset,
          applicationId,
          Set.empty,
          maxRecordTimeFromSubmissionTime,
        )
      ).thenReturn(
        Future.successful(Left(DeduplicationConversionFailure.CompletionOffsetNotMatching))
      )
      recoverToExceptionIf[StatusRuntimeException](callServiceWithDeduplicationPeriod(offsetPeriod))
        .map { result =>
          verify(
            periodConverter
          ).convertOffsetToDuration(
            offset,
            applicationId,
            Set.empty,
            maxRecordTimeFromSubmissionTime,
          )
          assertMatchesErrorCode(
            actual = result,
            LedgerApiErrors.RequestValidation.InvalidDeduplicationPeriodField,
          )
        }
    }

  }

  private def getFixture = new {
    val periodConverter: DeduplicationPeriodConverter = mock[DeduplicationPeriodConverter]
    val service = new DeduplicationPeriodSupport(
      periodConverter
    )
    val maxDeduplicationDuration = Duration.ofSeconds(5)
    val ledgerTimeModel = LedgerTimeModel.reasonableDefault
    val applicationId = Ref.ApplicationId.assertFromString("applicationid")
    val submittedAt = Instant.now()
    val maxRecordTimeFromSubmissionTime =
      ledgerTimeModel.maxRecordTime(Time.Timestamp.assertFromInstant(submittedAt)).toInstant
    val statusRuntimeException = new StatusRuntimeException(Status.OK)
    val deduplicationPeriodOffset =
      Offset.fromHexString(Hash.hashPrivateKey("offset").toHexString)
    val offset = deduplicationPeriodOffset.toHexString
    val durationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ofSeconds(1))
    val durationPeriodGreaterThanMax =
      DeduplicationPeriod.DeduplicationDuration(maxDeduplicationDuration.plusSeconds(1))

    def callServiceWithDeduplicationPeriod(
        offsetPeriod: DeduplicationPeriod
    ) = service
      .supportedDeduplicationPeriod(
        deduplicationPeriod = offsetPeriod,
        maxDeduplicationDuration,
        ledgerTimeModel,
        applicationId,
        Set.empty,
        submittedAt,
      )
  }
}
