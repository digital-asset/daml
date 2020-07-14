// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

import java.util.concurrent.atomic.AtomicInteger

import com.daml.caching.WeightedCache
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.caching.`Message Weight`
import com.daml.ledger.participant.state.kvutils.{Fingerprint, FingerprintPlaceholder}
import com.daml.ledger.validator.DefaultStateKeySerializationStrategy
import com.daml.ledger.validator.preexecution.DamlLedgerStateReaderWithFingerprints
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncWordSpec, Inside, Matchers}
import CachingDamlLedgerStateReaderWithFingerprints.`Message-Fingerprint Pair Weight`

import scala.concurrent.{ExecutionContext, Future}

class CachingDamlLedgerStateReaderWithFingerprintsSpec
    extends AsyncWordSpec
    with Matchers
    with Inside
    with MockitoSugar {
  "read" should {
    "update cache upon read if policy allows" in {
      val mockReader = mock[DamlLedgerStateReaderWithFingerprints]
      when(mockReader.read(argThat((keys: Seq[DamlStateKey]) => keys.size == 1)))
        .thenReturn(Future.successful(Seq((Some(aDamlStateValue()), FingerprintPlaceholder))))
      val instance = newInstance(mockReader, shouldCache = true)

      instance.read(Seq(aDamlStateKey)).map { _ =>
        instance.cache.getIfPresent(aDamlStateKey) shouldBe defined
      }
    }

    "do not update cache upon read if policy does not allow" in {
      val mockReader = mock[DamlLedgerStateReaderWithFingerprints]
      when(mockReader.read(argThat((keys: Seq[DamlStateKey]) => keys.size == 1)))
        .thenReturn(Future.successful(Seq((Some(aDamlStateValue()), FingerprintPlaceholder))))
      val instance = newInstance(mockReader, shouldCache = false)

      instance.read(Seq(aDamlStateKey)).map { _ =>
        instance.cache.getIfPresent(aDamlStateKey) should not be defined
      }
    }

    "serve request from cache for seen key (if policy allows)" in {
      val expectedStateValueFingerprintPair = None -> FingerprintPlaceholder
      val readCalledTimes = new AtomicInteger()
      val fakeReader =
        new DamlLedgerStateReaderWithFingerprints {
          override def read(
              keys: Seq[DamlStateKey]): Future[Seq[(Option[DamlStateValue], Fingerprint)]] = {
            readCalledTimes.incrementAndGet()
            Future.successful(keys.map(_ => expectedStateValueFingerprintPair))
          }
        }
      val instance = newInstance(fakeReader, shouldCache = true)

      for {
        originalReadState <- instance.read(Seq(aDamlStateKey))
        readAgain <- instance.read(Seq(aDamlStateKey))
      } yield {
        readCalledTimes.get() shouldBe 1
        originalReadState should have length 1
        originalReadState.head shouldBe expectedStateValueFingerprintPair
        readAgain shouldEqual originalReadState
      }
    }

    "return results for keys in the same order as requested" in {
      succeed
    }
  }
  private val keySerializationStrategy = DefaultStateKeySerializationStrategy

  private lazy val aDamlStateKey = DamlStateKey.newBuilder
    .setContractId("aContractId")
    .build

  private def aDamlStateValue(): DamlStateValue = DamlStateValue.getDefaultInstance

  private def newInstance(delegate: DamlLedgerStateReaderWithFingerprints, shouldCache: Boolean)(
      implicit executionContext: ExecutionContext): CachingDamlLedgerStateReaderWithFingerprints = {
    val cache = WeightedCache.from[DamlStateKey, (Option[DamlStateValue], Fingerprint)](
      WeightedCache.Configuration(1024))
    new CachingDamlLedgerStateReaderWithFingerprints(
      cache,
      _ => shouldCache,
      keySerializationStrategy,
      delegate)
  }

}
