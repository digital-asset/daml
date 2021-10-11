// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.ledger.offset.Offset
import com.google.protobuf.duration.Duration
import org.scalatest.Inside
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsCompletions
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AsyncFlatSpec =>

  behavior of "StorageBackend (completions)"

  import StorageBackendTestValues._

  it should "correctly find completions by offset range" in {
    val party = someParty
    val applicationId = someApplicationId

    val dtos = Vector(
      dtoConfiguration(offset(1)),
      dtoCompletion(offset(2), submitter = party),
      dtoCompletion(offset(3), submitter = party),
      dtoCompletion(offset(4), submitter = party),
    )

    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(4), 3L)))
      completions0to3 <- executeSql(
        backend.commandCompletions(Offset.beforeBegin, offset(3), applicationId, Set(party))
      )
      completions1to3 <- executeSql(
        backend.commandCompletions(offset(1), offset(3), applicationId, Set(party))
      )
      completions2to3 <- executeSql(
        backend.commandCompletions(offset(2), offset(3), applicationId, Set(party))
      )
      completions1to9 <- executeSql(
        backend.commandCompletions(offset(1), offset(9), applicationId, Set(party))
      )
    } yield {
      completions0to3 should have length 2
      completions1to3 should have length 2
      completions2to3 should have length 1
      completions1to9 should have length 3
    }
  }

  it should "correctly persist and retrieve application IDs" in {
    val party = someParty
    val applicationId = someApplicationId

    val dtos = Vector(
      dtoConfiguration(offset(1)),
      dtoCompletion(offset(2), submitter = party),
    )

    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(2), 1L)))
      completions <- executeSql(
        backend.commandCompletions(offset(1), offset(2), applicationId, Set(party))
      )
    } yield {
      completions should have length 1
      completions.head.completions should have length 1
      completions.head.completions.head.applicationId should be(applicationId)
    }
  }

  it should "correctly persist and retrieve submission IDs" in {
    val party = someParty
    val submissionId = Some(someSubmissionId)

    val dtos = Vector(
      dtoConfiguration(offset(1)),
      dtoCompletion(offset(2), submitter = party, submissionId = submissionId),
      dtoCompletion(offset(3), submitter = party, submissionId = None),
    )

    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(3), 2L)))
      completions <- executeSql(
        backend.commandCompletions(offset(1), offset(3), someApplicationId, Set(party))
      )
    } yield {
      completions should have length 2
      val List(completionWithSubmissionId, completionWithoutSubmissionId) = completions
      completionWithSubmissionId.completions should have length 1
      completionWithSubmissionId.completions.head.submissionId should be(someSubmissionId)
      completionWithoutSubmissionId.completions should have length 1
      completionWithoutSubmissionId.completions.head.submissionId should be("")
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

    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(3), 2L)))
      completions <- executeSql(
        backend.commandCompletions(offset(1), offset(3), someApplicationId, Set(party))
      )
    } yield {
      completions should have length 2
      val List(completionWithDeduplicationOffset, completionWithoutDeduplicationOffset) =
        completions
      completionWithDeduplicationOffset.completions should have length 1
      completionWithDeduplicationOffset.completions.head.deduplicationPeriod.deduplicationOffset should be(
        Some(anOffsetHex)
      )
      completionWithoutDeduplicationOffset.completions should have length 1
      completionWithoutDeduplicationOffset.completions.head.deduplicationPeriod.deduplicationOffset should not be defined
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

    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(3), 2L)))
      completions <- executeSql(
        backend.commandCompletions(offset(1), offset(3), someApplicationId, Set(party))
      )
    } yield {
      completions should have length 2
      val List(completionWithDeduplicationOffset, completionWithoutDeduplicationOffset) =
        completions
      completionWithDeduplicationOffset.completions should have length 1
      completionWithDeduplicationOffset.completions.head.deduplicationPeriod.deduplicationDuration should be(
        Some(expectedDuration)
      )
      completionWithoutDeduplicationOffset.completions should have length 1
      completionWithoutDeduplicationOffset.completions.head.deduplicationPeriod.deduplicationDuration should not be defined
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

    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos1, _))
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(2), 1L)))
      result <- executeSql(
        backend.commandCompletions(offset(1), offset(2), someApplicationId, Set(party))
      ).failed
    } yield {
      result shouldBe an[IllegalArgumentException]
      result.getMessage should be(expectedErrorMessage)
    }

    val dtos2 = Vector(
      dtoCompletion(
        offset(3),
        submitter = party,
        deduplicationDurationSeconds = None,
        deduplicationDurationNanos = Some(nanos),
      )
    )

    for {
      _ <- executeSql(ingest(dtos2, _))
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(3), 2L)))
      result <- executeSql(
        backend.commandCompletions(offset(2), offset(3), someApplicationId, Set(party))
      ).failed
    } yield {
      result shouldBe an[IllegalArgumentException]
      result.getMessage should be(expectedErrorMessage)
    }
  }
}
