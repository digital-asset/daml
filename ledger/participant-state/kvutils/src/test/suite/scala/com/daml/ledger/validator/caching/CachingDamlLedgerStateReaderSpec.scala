// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

import com.daml.caching.WeightedCache
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.caching.`Message Weight`
import com.daml.ledger.validator.ArgumentMatchers.{anyExecutionContext, seqOf}
import com.daml.ledger.validator.caching.CachingDamlLedgerStateReaderSpec._
import com.daml.ledger.validator.reading.DamlLedgerStateReader
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class CachingDamlLedgerStateReaderSpec
    extends AsyncWordSpec
    with Matchers
    with Inside
    with MockitoSugar
    with ArgumentMatchersSugar {
  "read" should {
    "update cache upon read if policy allows" in {
      val mockReader = mock[DamlLedgerStateReader]
      when(mockReader.read(seqOf(size = 1))(anyExecutionContext))
        .thenReturn(Future.successful(Seq(Some(aDamlStateValue))))
      val instance = newInstance(mockReader, shouldCache = true)

      instance.read(Seq(aDamlStateKey)).map { _ =>
        instance.cache.getIfPresent(aDamlStateKey) shouldBe defined
      }
    }

    "do not update cache upon read if policy does not allow" in {
      val mockReader = mock[DamlLedgerStateReader]
      when(mockReader.read(seqOf(size = 1))(anyExecutionContext))
        .thenReturn(Future.successful(Seq(Some(aDamlStateValue))))
      val instance = newInstance(mockReader, shouldCache = false)

      instance.read(Seq(aDamlStateKey)).map { _ =>
        instance.cache.getIfPresent(aDamlStateKey) should not be defined
      }
    }

    "serve request from cache for seen key (if policy allows)" in {
      val mockReader = mock[DamlLedgerStateReader]
      when(mockReader.read(any[Seq[DamlStateKey]])(anyExecutionContext))
        .thenReturn(Future.successful(Seq(Some(aDamlStateValue))))
      val instance = newInstance(mockReader, shouldCache = true)

      for {
        originalReadState <- instance.read(Seq(aDamlStateKey))
        readAgain <- instance.read(Seq(aDamlStateKey))
      } yield {
        verify(mockReader, times(1)).read(eqTo(Seq(aDamlStateKey)))(anyExecutionContext)
        readAgain shouldEqual originalReadState
      }
    }
  }
}

object CachingDamlLedgerStateReaderSpec {
  private lazy val aDamlStateKey = DamlStateKey.newBuilder
    .setContractId("aContractId")
    .build

  private val aDamlStateValue: DamlStateValue = DamlStateValue.getDefaultInstance

  private def newInstance(
      damlLedgerStateReader: DamlLedgerStateReader,
      shouldCache: Boolean,
  ): CachingDamlLedgerStateReader = {
    val cache = WeightedCache.from[DamlStateKey, DamlStateValue](WeightedCache.Configuration(1024))
    new CachingDamlLedgerStateReader(cache, _ => shouldCache, damlLedgerStateReader)
  }
}
