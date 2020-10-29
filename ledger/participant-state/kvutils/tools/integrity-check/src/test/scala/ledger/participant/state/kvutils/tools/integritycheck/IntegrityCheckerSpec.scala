// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import com.daml.ledger.participant.state.kvutils.export.WriteSet
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.google.protobuf.ByteString
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class IntegrityCheckerSpec extends WordSpec with Matchers with MockitoSugar {
  "compareSameSizeWriteSets" should {
    "return None in case strategy cannot explain difference" in {
      val mockCommitStrategySupport = mock[CommitStrategySupport[Unit]]
      when(mockCommitStrategySupport.explainMismatchingValue(any[Key], any[Value], any[Value]))
        .thenReturn(None)
      val instance = new IntegrityChecker[Unit](mockCommitStrategySupport)

      instance.compareSameSizeWriteSets(toWriteSet("key" -> "a"), toWriteSet("key" -> "b")) shouldBe None
    }

    "return None in case of no difference" in {
      val mockCommitStrategySupport = mock[CommitStrategySupport[Unit]]
      val instance = new IntegrityChecker[Unit](mockCommitStrategySupport)
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
      val mockCommitStrategySupport = mock[CommitStrategySupport[Unit]]
      val instance = new IntegrityChecker[Unit](mockCommitStrategySupport)

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

  private def countOccurrences(input: String, pattern: String): Int =
    input.sliding(pattern.length).count(_ == pattern)

  private def toWriteSet(values: (String, String)*): WriteSet =
    values.map {
      case (key, value) => ByteString.copyFromUtf8(key) -> ByteString.copyFromUtf8(value)
    }
}
