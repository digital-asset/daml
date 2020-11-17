// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.export.WriteSet
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.google.protobuf.ByteString
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.Future

class IntegrityCheckerSpec extends AsyncWordSpec with Matchers with MockitoSugar {
  "compareSameSizeWriteSets" should {
    "return None in case strategy cannot explain difference" in {
      val mockCommitStrategySupport = mock[CommitStrategySupport[Unit]]
      when(mockCommitStrategySupport.explainMismatchingValue(any[Key], any[Value], any[Value]))
        .thenReturn(None)
      val instance = new IntegrityChecker[Unit](mockCommitStrategySupport)

      instance.compareSameSizeWriteSets(toWriteSet("key" -> "a"), toWriteSet("key" -> "b")) shouldBe None
    }

    "return None in case of no difference" in {
      val instance = createMockIntegrityChecker()
      val aWriteSet = toWriteSet("key" -> "value")

      instance.compareSameSizeWriteSets(aWriteSet, aWriteSet) shouldBe None
    }

    "return explanation from strategy in case it can explain the difference" in {
      val mockCommitStrategySupport = mock[CommitStrategySupport[Unit]]
      when(mockCommitStrategySupport.explainMismatchingValue(any[Key], any[Value], any[Value]))
        .thenReturn(Some("expected explanation"))
      val instance = new IntegrityChecker[Unit](mockCommitStrategySupport)

      val actual =
        instance.compareSameSizeWriteSets(toWriteSet("key" -> "a"), toWriteSet("key" -> "b"))

      actual match {
        case Some(explanation) => explanation should include("expected explanation")
        case None => fail
      }
    }

    "return all explanations in case of multiple differences" in {
      val mockCommitStrategySupport = mock[CommitStrategySupport[Unit]]
      when(mockCommitStrategySupport.explainMismatchingValue(any[Key], any[Value], any[Value]))
        .thenReturn(Some("first explanation"), Some("second explanation"))
      val instance = new IntegrityChecker[Unit](mockCommitStrategySupport)

      val actual =
        instance.compareSameSizeWriteSets(
          toWriteSet("key1" -> "a", "key2" -> "a"),
          toWriteSet("key1" -> "b", "key2" -> "b"))

      actual match {
        case Some(explanation) =>
          explanation should include("first explanation")
          explanation should include("second explanation")
          // We output 3 lines per one pair of mismatching keys and we don't insert a new line
          // after the last.
          countOccurrences(explanation, System.lineSeparator()) shouldBe 2 * 3 - 1

        case None => fail
      }
    }

    "return differing keys" in {
      val instance = createMockIntegrityChecker()

      val actual =
        instance.compareSameSizeWriteSets(toWriteSet("key1" -> "a"), toWriteSet("key2" -> "b"))

      actual match {
        case Some(explanation) =>
          explanation should include("expected key")
          explanation should include("actual key")
          // We output 2 lines per one pair of mismatching keys.
          countOccurrences(explanation, System.lineSeparator()) shouldBe 1

        case None => fail
      }
    }
  }

  "compareStateUpdates" should {
    "call compare if not in index-only mode" in {
      val mockStateUpdates = mock[StateUpdates]
      when(mockStateUpdates.compare()).thenReturn(Future.unit)
      val config = Config.ParseInput.copy(indexOnly = false)
      val instance = createMockIntegrityChecker()

      instance
        .compareStateUpdates(config, mockStateUpdates)
        .transform(_ => {
          verify(mockStateUpdates, times(1)).compare()
          succeed
        }, _ => fail())
    }

    "skip compare if in index-only mode" in {
      val mockStateUpdates = mock[StateUpdates]
      when(mockStateUpdates.compare()).thenReturn(Future.unit)
      val config = Config.ParseInput.copy(indexOnly = true)
      val instance = createMockIntegrityChecker()

      instance
        .compareStateUpdates(config, mockStateUpdates)
        .transform(_ => {
          verify(mockStateUpdates, times(0)).compare()
          succeed
        }, _ => fail())
    }
  }

  "createIndexerConfig" should {
    "use the configured jdbcUrl if available" in {
      val configuredJdbcUrl = "aJdbcUrl"
      val config = Config.ParseInput.copy(jdbcUrl = Some(configuredJdbcUrl))
      IntegrityChecker.createIndexerConfig(config).jdbcUrl should be(configuredJdbcUrl)
    }

    "use the default jdbcUrl if none is configured" in {
      val aFilePath = "aFilePath"
      val config = Config.ParseInput.copy(exportFilePath = Paths.get(aFilePath))
      IntegrityChecker.createIndexerConfig(config).jdbcUrl should be(
        IntegrityChecker.defaultJdbcUrl(aFilePath))
    }
  }

  private def createMockIntegrityChecker(): IntegrityChecker[Unit] = {
    val mockCommitStrategySupport = mock[CommitStrategySupport[Unit]]
    val instance = new IntegrityChecker[Unit](mockCommitStrategySupport)
    instance
  }

  private def countOccurrences(input: String, pattern: String): Int =
    input.sliding(pattern.length).count(_ == pattern)

  private def toWriteSet(values: (String, String)*): WriteSet =
    values.map {
      case (key, value) => ByteString.copyFromUtf8(key) -> ByteString.copyFromUtf8(value)
    }

  private lazy val actorSystem: ActorSystem = ActorSystem("IntegrityCheckerSpec")
  private lazy implicit val materializer: Materializer = Materializer(actorSystem)
}
