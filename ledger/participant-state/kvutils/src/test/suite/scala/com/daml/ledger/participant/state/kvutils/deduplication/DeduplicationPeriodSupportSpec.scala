// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.deduplication

import java.time.{Duration, Instant}

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.TestLoggers
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.api.domain.ApplicationId
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.configuration.LedgerTimeModel
import com.daml.ledger.offset.Offset
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.platform.server.api.validation.{DeduplicationPeriodValidator, ErrorFactories}
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
    with ArgumentMatchersSugar {

  "using deduplication duration" should {
    "passthrough without validation" in {
      val fixture = getFixture
      import fixture._
      val period = DeduplicationPeriod.DeduplicationDuration(Duration.ofSeconds(1))
      callServiceWithDeduplicationPeriod(period)
        .map { result =>
          verifyNoMoreInteractions(periodConverter)
          verifyNoMoreInteractions(periodValidator)
          result shouldBe period
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
      val durationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ofSeconds(1))
      when(
        periodConverter.convertOffsetToDuration(
          offset,
          applicationId,
          Set.empty,
          maxRecordTimeFromSubmissionTime,
        )
      ).thenReturn(Future.successful(Right(durationPeriod.duration)))
      when(periodValidator.validate(durationPeriod, maxDeduplicationDuration))
        .thenReturn(Right(durationPeriod))
      callServiceWithDeduplicationPeriod(offsetPeriod)
        .map { result =>
          verify(periodConverter).convertOffsetToDuration(
            offset,
            applicationId,
            Set.empty,
            maxRecordTimeFromSubmissionTime,
          )
          verify(periodValidator).validate(durationPeriod, maxDeduplicationDuration)
          result shouldBe durationPeriod
        }
    }

    "convert offset and return validation failure" in {
      val fixture = getFixture
      import fixture._
      val offsetPeriod = DeduplicationPeriod.DeduplicationOffset(
        deduplicationPeriodOffset
      )
      val durationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ofSeconds(1))
      when(
        periodConverter.convertOffsetToDuration(
          offset,
          applicationId,
          Set.empty,
          maxRecordTimeFromSubmissionTime,
        )
      ).thenReturn(Future.successful(Right(durationPeriod.duration)))
      when(periodValidator.validate(durationPeriod, maxDeduplicationDuration))
        .thenReturn(Left(statusRuntimeException))
      recoverToExceptionIf[StatusRuntimeException](callServiceWithDeduplicationPeriod(offsetPeriod))
        .map { result =>
          verify(periodConverter).convertOffsetToDuration(
            offset,
            applicationId,
            Set.empty,
            maxRecordTimeFromSubmissionTime,
          )
          verify(periodValidator).validate(durationPeriod, maxDeduplicationDuration)
          result shouldBe statusRuntimeException
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
      when(
        errorFactories.invalidDeduplicationDuration(
          any[String],
          any[String],
          any[Option[Boolean]],
          any[Option[Duration]],
        )(any[ContextualizedErrorLogger])
      ).thenReturn(statusRuntimeException)
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
          verify(errorFactories).invalidDeduplicationDuration(
            any[String],
            any[String],
            any[Option[Boolean]],
            any[Option[Duration]],
          )(any[ContextualizedErrorLogger])
          result shouldBe statusRuntimeException
        }
    }

  }

  private def getFixture = new {
    val periodConverter: DeduplicationPeriodConverter = mock[DeduplicationPeriodConverter]
    val periodValidator: DeduplicationPeriodValidator = mock[DeduplicationPeriodValidator]
    val errorFactories: ErrorFactories = mock[ErrorFactories]
    val service = new DeduplicationPeriodSupport(
      periodConverter,
      periodValidator,
      errorFactories,
    )
    val maxDeduplicationDuration = Duration.ofSeconds(5)
    val ledgerTimeModel = LedgerTimeModel.reasonableDefault
    val applicationId = ApplicationId(Ref.LedgerString.assertFromString("applicationid"))
    val submittedAt = Instant.now()
    val maxRecordTimeFromSubmissionTime = submittedAt.plus(ledgerTimeModel.minSkew)
    val statusRuntimeException = new StatusRuntimeException(Status.OK)
    val deduplicationPeriodOffset =
      Offset.fromHexString(Hash.hashPrivateKey("offset").toHexString)
    val offset = deduplicationPeriodOffset.toHexString

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
