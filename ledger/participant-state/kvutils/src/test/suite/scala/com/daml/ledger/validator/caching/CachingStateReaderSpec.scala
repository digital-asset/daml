// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

import com.daml.caching.Cache.Size
import com.daml.caching.{Weight, WeightedCache}
import com.daml.ledger.validator.ArgumentMatchers.{anyExecutionContext, seqOf}
import com.daml.ledger.validator.caching.CachingStateReaderSpec._
import com.daml.ledger.validator.reading.StateReader
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class CachingStateReaderSpec
    extends AsyncWordSpec
    with Matchers
    with Inside
    with MockitoSugar
    with ArgumentMatchersSugar {
  "read" should {
    "update cache upon read if policy allows" in {
      val mockReader = mock[TestStateReader]
      when(mockReader.read(seqOf(size = 1))(anyExecutionContext))
        .thenReturn(Future.successful(Seq(Some(TestValue.random()))))
      val instance = newInstance(mockReader, shouldCache = true)

      instance.read(Seq(TestKey(1))).map { _ =>
        instance.cache.getIfPresent(TestKey(1)) shouldBe defined
      }
    }

    "do not update cache upon read if policy does not allow" in {
      val mockReader = mock[TestStateReader]
      when(mockReader.read(seqOf(size = 1))(anyExecutionContext))
        .thenReturn(Future.successful(Seq(Some(TestValue.random()))))
      val instance = newInstance(mockReader, shouldCache = false)

      instance.read(Seq(TestKey(2))).map { _ =>
        instance.cache.getIfPresent(TestKey(2)) should not be defined
      }
    }

    "serve request from cache for seen key (if policy allows)" in {
      val mockReader = mock[TestStateReader]
      when(mockReader.read(seqOf(size = 1))(anyExecutionContext))
        .thenReturn(Future.successful(Seq(Some(TestValue(7)))))
      val instance = newInstance(mockReader, shouldCache = true)

      for {
        originalReadState <- instance.read(Seq(TestKey(3)))
        readAgain <- instance.read(Seq(TestKey(3)))
      } yield {
        verify(mockReader, times(1)).read(eqTo(Seq(TestKey(3))))(anyExecutionContext)
        readAgain shouldEqual originalReadState
      }
    }
  }
}

object CachingStateReaderSpec {

  final case class TestKey(value: Long) {
    assert(value > 0)
  }

  implicit object `TestKey Weight` extends Weight[TestKey] {
    override def weigh(value: TestKey): Size = value.value
  }

  final case class TestValue(value: Long) {
    assert(value > 0)
  }

  object TestValue {
    def random(): TestValue = TestValue(4) // https://xkcd.com/221/
  }

  implicit object `TestValue Weight` extends Weight[TestValue] {
    override def weigh(value: TestValue): Size = value.value
  }

  type TestStateReader = StateReader[TestKey, TestValue]

  private def newInstance(
      reader: StateReader[TestKey, TestValue],
      shouldCache: Boolean,
  ): CachingStateReader[TestKey, TestValue] = {
    val cache = WeightedCache.from[TestKey, TestValue](WeightedCache.Configuration(1024))
    new CachingStateReader(cache, _ => shouldCache, reader)
  }
}
