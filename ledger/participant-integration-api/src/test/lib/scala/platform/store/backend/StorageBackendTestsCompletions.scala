// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.time.Duration

import com.daml.ledger.offset.Offset
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
      completionWithSubmissionId.completions.head.submissionId should be(submissionId)
      completionWithoutSubmissionId.completions should have length 1
      completionWithoutSubmissionId.completions.head.submissionId should be("")
    }
  }

  it should "correctly persist and retrieve command deduplication offsets" in {
    val party = someParty
    val anOffset = "someOffset"

    val dtos = Vector(
      dtoConfiguration(offset(1)),
      dtoCompletion(
        offset(2),
        submitter = party,
        deduplicationOffset = Some(anOffset),
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
        Some(anOffset)
      )
      completionWithoutDeduplicationOffset.completions should have length 1
      completionWithoutDeduplicationOffset.completions.head.deduplicationPeriod.deduplicationOffset should not be defined
    }
  }

  it should "correctly persist and retrieve command deduplication times" in {
    val party = someParty
    val seconds = 100L
    val nanos = 10
    val expectedDuration = Duration.ofSeconds(seconds).plusNanos(nanos.toLong)

    val dtos = Vector(
      dtoConfiguration(offset(1)),
      dtoCompletion(
        offset(2),
        submitter = party,
        deduplicationTimeSeconds = Some(seconds),
        deduplicationTimeNanos = Some(nanos),
      ),
      dtoCompletion(
        offset(3),
        submitter = party,
        deduplicationTimeSeconds = None,
        deduplicationTimeNanos = None,
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
      completionWithDeduplicationOffset.completions.head.deduplicationPeriod.deduplicationOffset should be(
        expectedDuration
      )
      completionWithoutDeduplicationOffset.completions should have length 1
      completionWithoutDeduplicationOffset.completions.head.deduplicationPeriod.deduplicationTime should not be defined
    }
  }

  it should "fail on broken command deduplication times in DB" in {
    val party = someParty
    val seconds = 100L
    val nanos = 10

    val dtos1 = Vector(
      dtoConfiguration(offset(1)),
      dtoCompletion(
        offset(2),
        submitter = party,
        deduplicationTimeSeconds = Some(seconds),
        deduplicationTimeNanos = None,
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
      result.getCause shouldBe an[IllegalArgumentException]
      result.getCause.getMessage should be(
        "One of deduplication time seconds and nanos has been provided " +
          "but they must be either both provided or both absent"
      )
    }

    val dtos2 = Vector(
      dtoCompletion(
        offset(3),
        submitter = party,
        deduplicationTimeSeconds = None,
        deduplicationTimeNanos = Some(nanos),
      )
    )

    for {
      _ <- executeSql(ingest(dtos2, _))
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(3), 2L)))
      result <- executeSql(
        backend.commandCompletions(offset(2), offset(3), someApplicationId, Set(party))
      ).failed
    } yield {
      result.getCause shouldBe an[IllegalArgumentException]
      result.getCause.getMessage should be(
        "One of deduplication time seconds and nanos has been provided " +
          "but they must be either both provided or both absent"
      )
    }
  }
}
