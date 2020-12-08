// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

import com.daml.caching.WeightedCache
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.caching.`Message Weight`
import com.daml.ledger.validator.{DamlLedgerStateReader, DefaultStateKeySerializationStrategy}
import org.mockito.ArgumentMatchers._
import org.mockito.MockitoSugar
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}

class CachingDamlLedgerStateReaderSpec
    extends AsyncWordSpec
    with Matchers
    with Inside
    with MockitoSugar {

  "readState" should {
    "record read keys" in {
      val mockReader = mock[DamlLedgerStateReader]
      when(mockReader.readState(argThat((keys: Seq[DamlStateKey]) => keys.size == 1)))
        .thenReturn(Future.successful(Seq(Some(aDamlStateValue()))))
      val instance = newInstance(mockReader, shouldCache = false)

      instance.readState(Seq(aDamlStateKey)).map { actual =>
        actual should have size 1
        instance.getReadSet should be(
          Set(keySerializationStrategy.serializeStateKey(aDamlStateKey)))
      }
    }

    "update cache upon read if policy allows" in {
      val mockReader = mock[DamlLedgerStateReader]
      when(mockReader.readState(argThat((keys: Seq[DamlStateKey]) => keys.size == 1)))
        .thenReturn(Future.successful(Seq(Some(aDamlStateValue()))))
      val instance = newInstance(mockReader, shouldCache = true)

      instance.readState(Seq(aDamlStateKey)).map { _ =>
        instance.cache.getIfPresent(aDamlStateKey) shouldBe defined
      }
    }

    "do not update cache upon read if policy does not allow" in {
      val mockReader = mock[DamlLedgerStateReader]
      when(mockReader.readState(argThat((keys: Seq[DamlStateKey]) => keys.size == 1)))
        .thenReturn(Future.successful(Seq(Some(aDamlStateValue()))))
      val instance = newInstance(mockReader, shouldCache = false)

      instance.readState(Seq(aDamlStateKey)).map { _ =>
        instance.cache.getIfPresent(aDamlStateKey) should not be defined
      }
    }

    "serve request from cache for seen key (if policy allows)" in {
      val mockReader = mock[DamlLedgerStateReader]
      when(mockReader.readState(any[Seq[DamlStateKey]]())).thenReturn(Future.successful(Seq(None)))
      val instance = newInstance(mockReader, shouldCache = true)

      for {
        originalReadState <- instance.readState(Seq(aDamlStateKey))
        readAgain <- instance.readState(Seq(aDamlStateKey))
      } yield {
        verify(mockReader, times(1)).readState(_)
        readAgain shouldEqual originalReadState
      }
    }
  }

  private val keySerializationStrategy = DefaultStateKeySerializationStrategy

  private lazy val aDamlStateKey = DamlStateKey.newBuilder
    .setContractId("aContractId")
    .build

  private def aDamlStateValue(): DamlStateValue = DamlStateValue.getDefaultInstance

  private def newInstance(damlLedgerStateReader: DamlLedgerStateReader, shouldCache: Boolean)(
      implicit executionContext: ExecutionContext): CachingDamlLedgerStateReader = {
    val cache = WeightedCache.from[DamlStateKey, DamlStateValue](WeightedCache.Configuration(1024))
    new CachingDamlLedgerStateReader(
      cache,
      _ => shouldCache,
      keySerializationStrategy,
      damlLedgerStateReader)
  }
}
