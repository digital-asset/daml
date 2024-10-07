// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.platform.indexer.parallel.{PostPublishData, PublishSource}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.google.protobuf.duration.Duration
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

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
        dtoCompletion(offset(1), submitters = Set(party)),
        dtoCompletion(offset(2), submitters = Set(party), traceContext = emptyTraceContext),
        dtoCompletion(
          offset(3),
          submitters = Set(party),
          traceContext = serializableTraceContext,
        ),
      )

      executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
      executeSql(ingest(dtos, _))
      executeSql(updateLedgerEnd(offset(3), 3L))
      val completions0to2 = executeSql(
        backend.completion
          .commandCompletions(Offset.beforeBegin, offset(2), applicationId, Set(party), limit = 10)
      )
      val completions1to2 = executeSql(
        backend.completion
          .commandCompletions(offset(1), offset(2), applicationId, Set(party), limit = 10)
      )
      val completions0to9 = executeSql(
        backend.completion
          .commandCompletions(Offset.beforeBegin, offset(9), applicationId, Set(party), limit = 10)
      )

      completions0to2 should have length 2
      completions1to2 should have length 1
      completions0to9 should have length 3

      completions0to9.head.completionResponse.completion.map(_.traceContext) shouldBe Some(None)
      completions0to9(1).completionResponse.completion.map(_.traceContext) shouldBe Some(None)
      completions0to9(2).completionResponse.completion.map(_.traceContext) shouldBe Some(
        Some(SerializableTraceContext(aTraceContext).toDamlProto)
      )
    }
  }

  it should "correctly persist and retrieve application IDs" in {
    val party = someParty
    val applicationId = someApplicationId

    val dtos = Vector(
      dtoCompletion(offset(1), submitters = Set(party))
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(1), 1L))

    val completions = executeSql(
      backend.completion
        .commandCompletions(Offset.beforeBegin, offset(1), applicationId, Set(party), limit = 10)
    )

    completions should not be empty
    completions.head.completionResponse.completion should not be empty
    completions.head.completionResponse.completion.toList.head.applicationId should be(
      applicationId
    )
  }

  it should "correctly persist and retrieve submission IDs" in {
    val party = someParty
    val submissionId = Some(someSubmissionId)

    val dtos = Vector(
      dtoCompletion(offset(1), submitters = Set(party), submissionId = submissionId),
      dtoCompletion(offset(2), submitters = Set(party), submissionId = None),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val completions = executeSql(
      backend.completion
        .commandCompletions(
          Offset.beforeBegin,
          offset(2),
          someApplicationId,
          Set(party),
          limit = 10,
        )
    ).toList

    completions should have length 2
    inside(completions) { case List(completionWithSubmissionId, completionWithoutSubmissionId) =>
      completionWithSubmissionId.completionResponse.completion should not be empty
      completionWithSubmissionId.completionResponse.completion.toList.head.submissionId should be(
        someSubmissionId
      )
      completionWithoutSubmissionId.completionResponse.completion should not be empty
      completionWithoutSubmissionId.completionResponse.completion.toList.head.submissionId should be(
        ""
      )
    }
  }

  it should "correctly persist and retrieve command deduplication offsets" in {
    val party = someParty
    val anOffsetHex = Offset.beforeBegin.toHexString

    val dtos = Vector(
      dtoCompletion(
        offset(1),
        submitters = Set(party),
        deduplicationOffset = Some(anOffsetHex),
      ),
      dtoCompletion(offset(2), submitters = Set(party), deduplicationOffset = None),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))

    executeSql(updateLedgerEnd(offset(2), 2L))
    val completions = executeSql(
      backend.completion
        .commandCompletions(
          Offset.beforeBegin,
          offset(2),
          someApplicationId,
          Set(party),
          limit = 10,
        )
    ).toList

    completions should have length 2
    inside(completions) {
      case List(completionWithDeduplicationOffset, completionWithoutDeduplicationOffset) =>
        completionWithDeduplicationOffset.completionResponse.completion should not be empty
        completionWithDeduplicationOffset.completionResponse.completion.toList.head.deduplicationPeriod.deduplicationOffset should be(
          Some(anOffsetHex)
        )
        completionWithoutDeduplicationOffset.completionResponse.completion should not be empty
        completionWithoutDeduplicationOffset.completionResponse.completion.toList.head.deduplicationPeriod.deduplicationOffset should not be defined
    }
  }

  it should "correctly persist and retrieve command deduplication durations" in {
    val party = someParty
    val seconds = 100L
    val nanos = 10
    val expectedDuration = Duration.of(seconds, nanos)

    val dtos = Vector(
      dtoCompletion(
        offset(1),
        submitters = Set(party),
        deduplicationDurationSeconds = Some(seconds),
        deduplicationDurationNanos = Some(nanos),
      ),
      dtoCompletion(
        offset(2),
        submitters = Set(party),
        deduplicationDurationSeconds = None,
        deduplicationDurationNanos = None,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))

    executeSql(updateLedgerEnd(offset(2), 2L))
    val completions = executeSql(
      backend.completion
        .commandCompletions(
          Offset.beforeBegin,
          offset(2),
          someApplicationId,
          Set(party),
          limit = 10,
        )
    ).toList

    completions should have length 2
    inside(completions) {
      case List(completionWithDeduplicationOffset, completionWithoutDeduplicationOffset) =>
        completionWithDeduplicationOffset.completionResponse.completion should not be empty
        completionWithDeduplicationOffset.completionResponse.completion.toList.head.deduplicationPeriod.deduplicationDuration should be(
          Some(expectedDuration)
        )
        completionWithoutDeduplicationOffset.completionResponse.completion should not be empty
        completionWithoutDeduplicationOffset.completionResponse.completion.toList.head.deduplicationPeriod.deduplicationDuration should not be defined
    }
  }

  it should "correctly persist and retrieve submitters/act_as" in {
    val party = someParty
    val party2 = someParty2
    val party3 = someParty3

    val dtos = Vector(
      dtoCompletion(
        offset(1),
        submitters = Set(party, party2, party3),
      ),
      dtoCompletion(
        offset(2),
        submitters = Set(party),
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))

    executeSql(updateLedgerEnd(offset(2), 2L))
    val completions = executeSql(
      backend.completion
        .commandCompletions(
          Offset.beforeBegin,
          offset(2),
          someApplicationId,
          Set(party, party2),
          limit = 10,
        )
    ).toList

    completions should have length 2
    inside(completions) { case List(completion1, completion2) =>
      completion1.completionResponse.completion should not be empty
      completion1.completionResponse.completion.toList.head.actAs.toSet should be(
        Set(party, party2)
      )
      completion2.completionResponse.completion should not be empty
      completion2.completionResponse.completion.toList.head.actAs.toSet should be(
        Set(party)
      )
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
      dtoCompletion(
        offset(1),
        submitters = Set(party),
        deduplicationDurationSeconds = Some(seconds),
        deduplicationDurationNanos = None,
      )
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos1, _))
    executeSql(updateLedgerEnd(offset(1), 1L))
    val caught = intercept[IllegalArgumentException](
      executeSql(
        backend.completion.commandCompletions(
          Offset.beforeBegin,
          offset(1),
          someApplicationId,
          Set(party),
          limit = 10,
        )
      )
    )

    caught.getMessage should be(expectedErrorMessage)

    val dtos2 = Vector(
      dtoCompletion(
        offset(2),
        submitters = Set(party),
        deduplicationDurationSeconds = None,
        deduplicationDurationNanos = Some(nanos),
      )
    )

    executeSql(ingest(dtos2, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val caught2 = intercept[IllegalArgumentException](
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
    caught2.getMessage should be(expectedErrorMessage)
  }

  it should "correctly retrieve completions for post processing recovery" in {
    val messageUuid = UUID.randomUUID()
    val commandId = UUID.randomUUID().toString
    val publicationTime = Timestamp.now()
    val recordTime = Timestamp.now().addMicros(15)
    val submissionId = UUID.randomUUID().toString
    val dtos = Vector(
      dtoCompletion(
        offset(1)
      ),
      dtoCompletion(
        offset = offset(2),
        submitters = Set(someParty),
        commandId = commandId,
        applicationId = "applicationid1",
        submissionId = Some(submissionId),
        domainId = "x::domain1",
        messageUuid = Some(messageUuid.toString),
        publicationTime = publicationTime,
        isTransaction = true,
      ),
      dtoCompletion(
        offset = offset(5),
        submitters = Set(someParty),
        commandId = commandId,
        applicationId = "applicationid1",
        submissionId = Some(submissionId),
        domainId = "x::domain1",
        messageUuid = Some(messageUuid.toString),
        publicationTime = publicationTime,
        isTransaction = false,
      ),
      dtoCompletion(
        offset = offset(9),
        submitters = Set(someParty),
        commandId = commandId,
        applicationId = "applicationid1",
        submissionId = Some(submissionId),
        domainId = "x::domain1",
        recordTime = recordTime,
        messageUuid = None,
        transactionId = None,
        publicationTime = publicationTime,
        isTransaction = true,
        requestSequencerCounter = Some(11),
      ),
      dtoCompletion(
        offset = offset(11),
        submitters = Set(someParty),
        commandId = commandId,
        applicationId = "applicationid1",
        submissionId = Some(submissionId),
        domainId = "x::domain1",
        recordTime = recordTime,
        messageUuid = None,
        transactionId = None,
        publicationTime = publicationTime,
        isTransaction = true,
        requestSequencerCounter = None,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      backend.completion.commandCompletionsForRecovery(offset(1), offset(10))
    ) shouldBe Vector(
      PostPublishData(
        submissionDomainId = DomainId.tryFromString("x::domain1"),
        publishSource = PublishSource.Local(messageUuid),
        applicationId = Ref.ApplicationId.assertFromString("applicationid1"),
        commandId = Ref.CommandId.assertFromString(commandId),
        actAs = Set(someParty),
        offset = offset(2),
        publicationTime = CantonTimestamp(publicationTime),
        submissionId = Some(Ref.SubmissionId.assertFromString(submissionId)),
        accepted = true,
        traceContext = TraceContext.empty,
      ),
      PostPublishData(
        submissionDomainId = DomainId.tryFromString("x::domain1"),
        publishSource = PublishSource.Sequencer(
          requestSequencerCounter = SequencerCounter(11),
          sequencerTimestamp = CantonTimestamp(recordTime),
        ),
        applicationId = Ref.ApplicationId.assertFromString("applicationid1"),
        commandId = Ref.CommandId.assertFromString(commandId),
        actAs = Set(someParty),
        offset = offset(9),
        publicationTime = CantonTimestamp(publicationTime),
        submissionId = Some(Ref.SubmissionId.assertFromString(submissionId)),
        accepted = false,
        traceContext = TraceContext.empty,
      ),
    )

    // this tries to deserialize the last dto which has an invalid combination of message_uuid and request_sequencer_counter
    intercept[IllegalStateException](
      executeSql(backend.completion.commandCompletionsForRecovery(offset(2), offset(11)))
    ).getMessage should include("if message_uuid is empty, this field should be populated")
  }
}
