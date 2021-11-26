// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.deduplication

import java.time.{Duration, Instant}

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.TestLoggers
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.api.domain.ApplicationId
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.offset.Offset
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.platform.server.api.validation.{DeduplicationPeriodValidator, ErrorFactories}
import io.grpc.{Status, StatusRuntimeException}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class DeduplicationPeriodServiceSpec
    extends AsyncWordSpec
    with MockitoSugar
    with Matchers
    with BeforeAndAfterEach
    with AkkaBeforeAndAfterAll
    with TestLoggers
    with ArgumentMatchersSugar {
  private val periodConverter: DeduplicationPeriodConverter = mock[DeduplicationPeriodConverter]
  private val periodValidator: DeduplicationPeriodValidator = mock[DeduplicationPeriodValidator]
  private val errorFactories: ErrorFactories = mock[ErrorFactories]
  private val service = new DeduplicationPeriodService(
    periodConverter,
    periodValidator,
    errorFactories,
  )
  private val maxDeduplicationDuration = Duration.ofSeconds(5)
  private val applicationId = ApplicationId(Ref.LedgerString.assertFromString("applicationid"))
  private val submittedAt = Instant.now()
  private val statusRuntimeException = new StatusRuntimeException(Status.OK)
  private val deduplicationPeriodOffset =
    Offset.fromHexString(Hash.hashPrivateKey("offset").toHexString)
  private val offset = deduplicationPeriodOffset.toHexString

  override protected def afterEach(): Unit = {
    reset(periodConverter, periodValidator, errorFactories)
  }

  "using deduplication duration" should {
    "validate and return it" in {
      val period = DeduplicationPeriod.DeduplicationDuration(Duration.ofSeconds(1))
      when(periodValidator.validate(period, maxDeduplicationDuration)).thenReturn(Right(period))
      callServiceWithDeduplicationPeriod(period)
        .map { result =>
          verify(periodValidator).validate(period, maxDeduplicationDuration)
          verifyNoMoreInteractions(periodConverter)
          result shouldBe period
        }
    }

    "return validation failure" in {
      val period = DeduplicationPeriod.DeduplicationDuration(Duration.ofSeconds(1))
      when(periodValidator.validate(period, maxDeduplicationDuration))
        .thenReturn(Left(statusRuntimeException))
      recoverToExceptionIf[StatusRuntimeException](callServiceWithDeduplicationPeriod(period))
        .map { result =>
          verify(periodValidator).validate(period, maxDeduplicationDuration)
          result shouldBe statusRuntimeException
        }
    }
  }

  "using deduplication offset" should {
    "convert offset and validate result" in {
      val offsetPeriod = DeduplicationPeriod.DeduplicationOffset(
        deduplicationPeriodOffset
      )
      val durationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ofSeconds(1))
      when(
        periodConverter.convertOffsetToDuration(
          offset,
          applicationId,
          Set.empty,
          submittedAt,
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
            submittedAt,
          )
          verify(periodValidator).validate(durationPeriod, maxDeduplicationDuration)
          result shouldBe durationPeriod
        }
    }

    "convert offset and return validation failure" in {
      val offsetPeriod = DeduplicationPeriod.DeduplicationOffset(
        deduplicationPeriodOffset
      )
      val durationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ofSeconds(1))
      when(
        periodConverter.convertOffsetToDuration(
          offset,
          applicationId,
          Set.empty,
          submittedAt,
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
            submittedAt,
          )
          verify(periodValidator).validate(durationPeriod, maxDeduplicationDuration)
          result shouldBe statusRuntimeException
        }
    }

    "fail to convert offset and return failure" in {
      val offsetPeriod = DeduplicationPeriod.DeduplicationOffset(
        deduplicationPeriodOffset
      )
      when(
        periodConverter.convertOffsetToDuration(
          offset,
          applicationId,
          Set.empty,
          submittedAt,
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
          ).convertOffsetToDuration(offset, applicationId, Set.empty, submittedAt)
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

  private def callServiceWithDeduplicationPeriod(
      offsetPeriod: DeduplicationPeriod
  ) = {
    service
      .supportedDeduplicationPeriod(
        deduplicationPeriod = offsetPeriod,
        maxDeduplicationDuration,
        applicationId,
        Set.empty,
        submittedAt,
      )
  }
}
