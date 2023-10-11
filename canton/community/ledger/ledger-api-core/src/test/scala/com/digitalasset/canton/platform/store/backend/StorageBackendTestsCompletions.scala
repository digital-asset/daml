// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.google.protobuf.duration.Duration
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsCompletions
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (completions)"

  import StorageBackendTestValues.*

  it should "correctly find completions by offset range" in {
    TraceContext.withNewTraceContext { aTraceContext =>
      val party = someParty
      val applicationId = someApplicationId
      val emptyTraceContext = SerializableTraceContext(TraceContext.empty).toDamlProto.toByteArray
      val serializableTraceContext = SerializableTraceContext(aTraceContext).toDamlProto.toByteArray

      val dtos = Vector(
        dtoConfiguration(offset(1)),
        dtoCompletion(offset(2), submitter = party),
        dtoCompletion(offset(3), submitter = party, traceContext = emptyTraceContext),
        dtoCompletion(
          offset(4),
          submitter = party,
          traceContext = serializableTraceContext,
        ),
      )

      executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
      executeSql(ingest(dtos, _))
      executeSql(updateLedgerEnd(offset(4), 3L))
      val completions0to3 = executeSql(
        backend.completion
          .commandCompletions(Offset.beforeBegin, offset(3), applicationId, Set(party), limit = 10)
      )
      val completions1to3 = executeSql(
        backend.completion
          .commandCompletions(offset(1), offset(3), applicationId, Set(party), limit = 10)
      )
      val completions2to3 = executeSql(
        backend.completion
          .commandCompletions(offset(2), offset(3), applicationId, Set(party), limit = 10)
      )
      val completions1to9 = executeSql(
        backend.completion
          .commandCompletions(offset(1), offset(9), applicationId, Set(party), limit = 10)
      )

      completions0to3 should have length 2
      completions1to3 should have length 2
      completions2to3 should have length 1
      completions1to9 should have length 3

      completions1to9.head.completion.map(_.traceContext) shouldBe Some(None)
      completions1to9(1).completion.map(_.traceContext) shouldBe Some(None)
      completions1to9(2).completion.map(_.traceContext) shouldBe Some(
        Some(SerializableTraceContext(aTraceContext).toDamlProto)
      )
    }
  }

  it should "correctly persist and retrieve application IDs" in {
    val party = someParty
    val applicationId = someApplicationId

    val dtos = Vector(
      dtoConfiguration(offset(1)),
      dtoCompletion(offset(2), submitter = party),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 1L))

    val completions = executeSql(
      backend.completion
        .commandCompletions(offset(1), offset(2), applicationId, Set(party), limit = 10)
    )

    completions should not be empty
    completions.head.completion should not be empty
    completions.head.completion.toList.head.applicationId should be(applicationId)
  }

  it should "correctly persist and retrieve submission IDs" in {
    val party = someParty
    val submissionId = Some(someSubmissionId)

    val dtos = Vector(
      dtoConfiguration(offset(1)),
      dtoCompletion(offset(2), submitter = party, submissionId = submissionId),
      dtoCompletion(offset(3), submitter = party, submissionId = None),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(3), 2L))
    val completions = executeSql(
      backend.completion
        .commandCompletions(offset(1), offset(3), someApplicationId, Set(party), limit = 10)
    ).toList

    completions should have length 2
    inside(completions) { case List(completionWithSubmissionId, completionWithoutSubmissionId) =>
      completionWithSubmissionId.completion should not be empty
      completionWithSubmissionId.completion.toList.head.submissionId should be(someSubmissionId)
      completionWithoutSubmissionId.completion should not be empty
      completionWithoutSubmissionId.completion.toList.head.submissionId should be("")
    }
  }

  it should "correctly persist and retrieve command deduplication offsets" in {
    val party = someParty
    val anOffsetHex = offset(0).toHexString

    val dtos = Vector(
      dtoConfiguration(offset(1)),
      dtoCompletion(
        offset(2),
        submitter = party,
        deduplicationOffset = Some(anOffsetHex),
      ),
      dtoCompletion(offset(3), submitter = party, deduplicationOffset = None),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))

    executeSql(updateLedgerEnd(offset(3), 2L))
    val completions = executeSql(
      backend.completion
        .commandCompletions(offset(1), offset(3), someApplicationId, Set(party), limit = 10)
    ).toList

    completions should have length 2
    inside(completions) {
      case List(completionWithDeduplicationOffset, completionWithoutDeduplicationOffset) =>
        completionWithDeduplicationOffset.completion should not be empty
        completionWithDeduplicationOffset.completion.toList.head.deduplicationPeriod.deduplicationOffset should be(
          Some(anOffsetHex)
        )
        completionWithoutDeduplicationOffset.completion should not be empty
        completionWithoutDeduplicationOffset.completion.toList.head.deduplicationPeriod.deduplicationOffset should not be defined
    }
  }

  it should "correctly persist and retrieve command deduplication durations" in {
    val party = someParty
    val seconds = 100L
    val nanos = 10
    val expectedDuration = Duration.of(seconds, nanos)

    val dtos = Vector(
      dtoConfiguration(offset(1)),
      dtoCompletion(
        offset(2),
        submitter = party,
        deduplicationDurationSeconds = Some(seconds),
        deduplicationDurationNanos = Some(nanos),
      ),
      dtoCompletion(
        offset(3),
        submitter = party,
        deduplicationDurationSeconds = None,
        deduplicationDurationNanos = None,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))

    executeSql(updateLedgerEnd(offset(3), 2L))
    val completions = executeSql(
      backend.completion
        .commandCompletions(offset(1), offset(3), someApplicationId, Set(party), limit = 10)
    ).toList

    completions should have length 2
    inside(completions) {
      case List(completionWithDeduplicationOffset, completionWithoutDeduplicationOffset) =>
        completionWithDeduplicationOffset.completion should not be empty
        completionWithDeduplicationOffset.completion.toList.head.deduplicationPeriod.deduplicationDuration should be(
          Some(expectedDuration)
        )
        completionWithoutDeduplicationOffset.completion should not be empty
        completionWithoutDeduplicationOffset.completion.toList.head.deduplicationPeriod.deduplicationDuration should not be defined
    }
  }

  it should "fail on broken command deduplication durations in DB" in {
    val party = someParty
    val seconds = 100L
    val nanos = 10

    val expectedErrorMessage =
      "One of deduplication duration seconds and nanos has been provided " +
        "but they must be either both provided or both absent"

    val dtos1 = Vector(
      dtoConfiguration(offset(1)),
      dtoCompletion(
        offset(2),
        submitter = party,
        deduplicationDurationSeconds = Some(seconds),
        deduplicationDurationNanos = None,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos1, _))
    executeSql(updateLedgerEnd(offset(2), 1L))
    val caught = intercept[IllegalArgumentException](
      executeSql(
        backend.completion.commandCompletions(
          offset(1),
          offset(2),
          someApplicationId,
          Set(party),
          limit = 10,
        )
      )
    )

    caught.getMessage should be(expectedErrorMessage)

    val dtos2 = Vector(
      dtoCompletion(
        offset(3),
        submitter = party,
        deduplicationDurationSeconds = None,
        deduplicationDurationNanos = Some(nanos),
      )
    )

    executeSql(ingest(dtos2, _))
    executeSql(updateLedgerEnd(offset(3), 2L))
    val caught2 = intercept[IllegalArgumentException](
      executeSql(
        backend.completion.commandCompletions(
          offset(2),
          offset(3),
          someApplicationId,
          Set(party),
          limit = 10,
        )
      )
    )
    caught2.getMessage should be(expectedErrorMessage)
  }
}
