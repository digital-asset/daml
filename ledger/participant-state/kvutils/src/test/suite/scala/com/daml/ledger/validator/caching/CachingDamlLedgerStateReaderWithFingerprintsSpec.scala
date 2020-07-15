// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

import java.util.concurrent.atomic.AtomicInteger

import com.daml.caching.WeightedCache
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlPartyAllocation,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.caching.`Message Weight`
import com.daml.ledger.participant.state.kvutils.{Fingerprint, FingerprintPlaceholder}
import com.daml.ledger.validator.DefaultStateKeySerializationStrategy
import com.daml.ledger.validator.caching.CachingDamlLedgerStateReaderWithFingerprints.`Message-Fingerprint Pair Weight`
import com.daml.ledger.validator.preexecution.DamlLedgerStateReaderWithFingerprints
import org.mockito.ArgumentMatchers.argThat
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncWordSpec, Inside, Matchers}

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

      instance.read(Seq(aDamlStateKey())).map { _ =>
        instance.cache.getIfPresent(aDamlStateKey()) shouldBe defined
      }
    }

    "do not update cache upon read if policy does not allow" in {
      val mockReader = mock[DamlLedgerStateReaderWithFingerprints]
      when(mockReader.read(argThat((keys: Seq[DamlStateKey]) => keys.size == 1)))
        .thenReturn(Future.successful(Seq((Some(aDamlStateValue()), FingerprintPlaceholder))))
      val instance = newInstance(mockReader, shouldCache = false)

      instance.read(Seq(aDamlStateKey())).map { _ =>
        instance.cache.getIfPresent(aDamlStateKey()) should not be defined
      }
    }

    "serve request from cache for seen key (if policy allows)" in {
      val expectedStateValueFingerprintPair = Some(aDamlStateValue()) -> FingerprintPlaceholder
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
        originalReadState <- instance.read(Seq(aDamlStateKey()))
        readAgain <- instance.read(Seq(aDamlStateKey()))
      } yield {
        readCalledTimes.get() shouldBe 1
        originalReadState should have length 1
        originalReadState.head shouldBe expectedStateValueFingerprintPair
        readAgain shouldEqual originalReadState
      }
    }

    "do not cache None value returned from delegate" in {
      val mockReader = mock[DamlLedgerStateReaderWithFingerprints]
      when(mockReader.read(argThat((keys: Seq[DamlStateKey]) => keys.size == 1)))
        .thenReturn(Future.successful(Seq((None, FingerprintPlaceholder))))
      val instance = newInstance(mockReader, shouldCache = true)

      instance.read(Seq(aDamlStateKey())).map { _ =>
        instance.cache.getIfPresent(aDamlStateKey()) should not be defined
      }
    }

    "return results for keys in the same order as requested" in {
      val expectedKeyValues = (0 to 10).map { index =>
        (aDamlStateKey(index), aDamlStateValue(index))
      }
      val expectedKeys = expectedKeyValues.map(_._1)
      val expectedStateValueFingerprintPairs = expectedKeyValues.map {
        case (_, value) =>
          Some(value) -> FingerprintPlaceholder
      }
      val fakeReader = new DamlLedgerStateReaderWithFingerprints {
        override def read(
            keys: Seq[DamlStateKey]): Future[Seq[(Option[DamlStateValue], Fingerprint)]] =
          Future.successful {
            keys.map { key =>
              expectedStateValueFingerprintPairs(key.getContractId.toInt)
            }
          }
      }
      val instance = newInstance(fakeReader, shouldCache = true)

      for {
        originalReadState <- instance.read(expectedKeys)
        readAgain <- instance.read(expectedKeys)
      } yield {
        originalReadState should have length expectedKeyValues.length.toLong
        readAgain shouldEqual expectedStateValueFingerprintPairs
      }
    }
  }

  private val keySerializationStrategy = DefaultStateKeySerializationStrategy

  private def aDamlStateKey(id: Int = 0): DamlStateKey =
    DamlStateKey.newBuilder
      .setContractId(id.toString)
      .build

  private def aDamlStateValue(id: Int = 0): DamlStateValue =
    DamlStateValue.newBuilder
      .setParty(
        DamlPartyAllocation.newBuilder
          .setDisplayName(id.toString)
          .setParticipantId(id.toString))
      .build
  private def aDamlStateValue(): DamlStateValue = DamlStateValue.getDefaultInstance

  private def newInstance(delegate: DamlLedgerStateReaderWithFingerprints, shouldCache: Boolean)(
      implicit executionContext: ExecutionContext): CachingDamlLedgerStateReaderWithFingerprints = {
    val cache = WeightedCache.from[DamlStateKey, (DamlStateValue, Fingerprint)](
      WeightedCache.Configuration(1024))
    new CachingDamlLedgerStateReaderWithFingerprints(
      cache,
      _ => shouldCache,
      keySerializationStrategy,
      delegate)
  }
}
