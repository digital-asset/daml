// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

import com.daml.caching.{Cache, Weight, WeightedCache}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.{Fingerprint, FingerprintPlaceholder}
import com.daml.ledger.participant.state.kvutils.caching.`Message Weight`
import com.daml.ledger.validator.DefaultStateKeySerializationStrategy
import com.daml.ledger.validator.preexecution.DamlLedgerStateReaderWithFingerprints
import com.google.protobuf.MessageLite
import org.mockito.ArgumentMatchers.argThat
import org.mockito.Mockito.{times, verify, when}
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
      val mockReader = mock[DamlLedgerStateReaderWithFingerprints]
      when(mockReader.read(argThat((keys: Seq[DamlStateKey]) => keys.size == 1)))
        .thenReturn(Future.successful(Seq((None, FingerprintPlaceholder))))
      val instance = newInstance(mockReader, shouldCache = true)

      for {
        originalReadState <- instance.read(Seq(aDamlStateKey))
        readAgain <- instance.read(Seq(aDamlStateKey))
      } yield {
        verify(mockReader, times(1)).read(_)
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

  implicit object `Message-fingerprint Pair Weight` extends Weight[(MessageLite, Fingerprint)] {
    override def weigh(value: (MessageLite, Fingerprint)): Cache.Size =
      value._1.getSerializedSize.toLong
  }

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
