// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

import com.daml.caching.{Cache, WeightedCache}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlLogEntry, DamlLogEntryId}
import com.daml.ledger.participant.state.kvutils.caching.`Message Weight`
import com.daml.ledger.participant.state.kvutils.export.SubmissionAggregator
import com.daml.ledger.participant.state.kvutils.store.{DamlStateKey, DamlStateValue}
import com.daml.ledger.validator.CommitStrategy
import com.daml.ledger.validator.TestHelper._
import com.daml.ledger.validator.caching.CachingCommitStrategySpec._
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}

class CachingCommitStrategySpec extends AsyncWordSpec with Matchers with MockitoSugar {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "commit" should {
    "update cache with output state upon commit if policy allows" in {
      val cache = newCache()
      val instance = createInstance(cache, shouldCache = true)
      val expectedKey = DamlStateKey.newBuilder.setContractId("a contract ID").build
      val outputState = Map(expectedKey -> DamlStateValue.getDefaultInstance)

      instance
        .commit(aParticipantId, "correlation ID", aLogEntryId(), aLogEntry, Map.empty, outputState)
        .map { _ =>
          cache.getIfPresent(expectedKey) shouldBe defined
        }
    }

    "not update cache with output state upon commit if policy does not allow" in {
      val cache = newCache()
      val instance = createInstance(cache, shouldCache = false)
      val expectedKey = DamlStateKey.newBuilder.setContractId("a contract ID").build
      val outputState = Map(expectedKey -> DamlStateValue.getDefaultInstance)

      instance
        .commit(aParticipantId, "correlation ID", aLogEntryId(), aLogEntry, Map.empty, outputState)
        .map { _ =>
          cache.getIfPresent(expectedKey) should not be defined
        }
    }
  }
}

object CachingCommitStrategySpec {

  import ArgumentMatchersSugar._
  import MockitoSugar._

  private def newCache(): Cache[DamlStateKey, DamlStateValue] =
    WeightedCache.from[DamlStateKey, DamlStateValue](WeightedCache.Configuration(1024))

  private def createInstance(
      cache: Cache[DamlStateKey, DamlStateValue],
      shouldCache: Boolean,
  )(implicit executionContext: ExecutionContext): CachingCommitStrategy[Unit] = {
    val mockCommitStrategy = mock[CommitStrategy[Unit]]
    when(
      mockCommitStrategy.commit(
        any[Ref.ParticipantId],
        any[String],
        any[DamlLogEntryId],
        any[DamlLogEntry],
        any[Map[DamlStateKey, Option[DamlStateValue]]],
        any[Map[DamlStateKey, DamlStateValue]],
        any[Option[SubmissionAggregator.WriteSetBuilder]],
      )(any[LoggingContext])
    )
      .thenReturn(Future.unit)
    new CachingCommitStrategy[Unit](cache, _ => shouldCache, mockCommitStrategy)
  }
}
