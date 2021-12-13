// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.deduplication

import java.time.{Duration, Instant}

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.TestLoggers
import com.daml.ledger.api.domain.{ApplicationId, LedgerOffset}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.v1.command_completion_service.{Checkpoint, CompletionStreamResponse}
import com.daml.ledger.api.v1.ledger_offset
import com.daml.ledger.participant.state.index.v2.IndexCompletionsService
import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.lf.data.Ref
import com.google.protobuf.timestamp.Timestamp
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class CompletionBasedDeduplicationPeriodConverterSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with BeforeAndAfterEach
    with AkkaBeforeAndAfterAll
    with TestLoggers {

  private val indexCompletionsService: IndexCompletionsService = mock[IndexCompletionsService]
  private val deduplicationPeriodConverter =
    new CompletionBasedDeduplicationPeriodConverter(indexCompletionsService)
  private val offset = Ref.HexString.assertFromString("012345ffff")
  private val lowerOffset = Ref.HexString.assertFromString("012345fffe")
  private val applicationId = ApplicationId(Ref.ApplicationId.assertFromString("id"))
  private val parties = Set.empty[Ref.Party]
  private val emptyResponse = CompletionStreamResponse()

  override protected def afterEach(): Unit = reset(indexCompletionsService)

  "return expected duration" in {
    val submittedAt = Instant.now()
    val response = Source.single(
      emptyResponse.update(
        _.checkpoint.update(
          _.recordTime := Timestamp.fromJavaProto(
            Conversions.buildTimestamp(submittedAt.minusSeconds(1))
          ),
          _.offset := ledger_offset.LedgerOffset(ledger_offset.LedgerOffset.Value.Absolute(offset)),
        )
      )
    )
    completionServiceReturnsResponse(response)
    deduplicationPeriodConverter
      .convertOffsetToDuration(
        offset,
        applicationId,
        parties,
        submittedAt,
      )
      .map { result =>
        result shouldBe Right(Duration.ofSeconds(1))
      }
  }

  "return failure when there is an empty response" in {
    completionServiceReturnsResponse(Source.empty)
    deduplicationPeriodConverter
      .convertOffsetToDuration(
        offset,
        applicationId,
        parties,
        Instant.now(),
      )
      .map { result =>
        result shouldBe Left(DeduplicationConversionFailure.CompletionAtOffsetNotFound)
      }
  }

  "return failure when the checkpoint is missing" in {
    completionServiceReturnsResponse(Source.single(emptyResponse))
    deduplicationPeriodConverter
      .convertOffsetToDuration(
        offset,
        applicationId,
        parties,
        Instant.now(),
      )
      .map { result =>
        result shouldBe Left(DeduplicationConversionFailure.CompletionCheckpointNotAvailable)
      }
  }

  "return failure when the checkpoint misses the record time" in {
    completionServiceReturnsResponse(
      Source.single(
        emptyResponse.update(
          _.checkpoint.offset := ledger_offset.LedgerOffset(
            ledger_offset.LedgerOffset.Value.Absolute(offset)
          )
        )
      )
    )
    deduplicationPeriodConverter
      .convertOffsetToDuration(
        offset,
        applicationId,
        parties,
        Instant.now(),
      )
      .map { result =>
        result shouldBe Left(DeduplicationConversionFailure.CompletionRecordTimeNotAvailable)
      }
  }

  "return failure when the offset is missing" in {
    completionServiceReturnsResponse(
      Source.single(
        emptyResponse.update(_.checkpoint := Checkpoint.defaultInstance)
      )
    )
    deduplicationPeriodConverter
      .convertOffsetToDuration(
        offset,
        applicationId,
        parties,
        Instant.now(),
      )
      .map { result =>
        result shouldBe Left(DeduplicationConversionFailure.CompletionOffsetNotMatching)
      }
  }

  "return failure when the offset has a different value" in {
    completionServiceReturnsResponse(
      Source.single(
        emptyResponse.update(
          _.checkpoint.offset := ledger_offset.LedgerOffset(
            ledger_offset.LedgerOffset.Value.Absolute("another")
          )
        )
      )
    )
    deduplicationPeriodConverter
      .convertOffsetToDuration(
        offset,
        applicationId,
        parties,
        Instant.now(),
      )
      .map { result =>
        result shouldBe Left(DeduplicationConversionFailure.CompletionOffsetNotMatching)
      }
  }

  private def completionServiceReturnsResponse(
      response: Source[CompletionStreamResponse, NotUsed]
  ) = when(
    indexCompletionsService.getCompletions(
      LedgerOffset.Absolute(lowerOffset),
      LedgerOffset.Absolute(offset),
      applicationId,
      parties,
    )(loggingContext)
  ).thenReturn(
    response
  )
}
