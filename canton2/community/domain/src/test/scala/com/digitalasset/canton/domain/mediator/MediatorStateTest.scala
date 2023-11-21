// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.*
import com.digitalasset.canton.domain.mediator.store.{
  InMemoryFinalizedResponseStore,
  InMemoryMediatorDeduplicationStore,
  MediatorState,
}
import com.digitalasset.canton.domain.metrics.DomainTestMetrics
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.InformeeMessage
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.TrustLevel
import com.digitalasset.canton.topology.{DefaultTestIdentities, MediatorRef}
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPartyId}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

class MediatorStateTest
    extends AsyncWordSpec
    with BaseTest
    with HasTestCloseContext
    with HasExecutionContext { self =>

  "MediatorState" when {
    val requestId = RequestId(CantonTimestamp.Epoch)
    val fullInformeeTree = {
      val domainId = DefaultTestIdentities.domainId
      val mediatorId = DefaultTestIdentities.mediator
      val alice = PlainInformee(LfPartyId.assertFromString("alice"))
      val bob = ConfirmingParty(
        LfPartyId.assertFromString("bob"),
        PositiveInt.tryCreate(2),
        TrustLevel.Ordinary,
      )
      val hashOps: HashOps = new SymbolicPureCrypto
      val h: Int => Hash = TestHash.digest
      val s: Int => Salt = TestSalt.generateSalt
      def rh(index: Int): RootHash = RootHash(h(index))
      val viewCommonData =
        ViewCommonData.create(hashOps)(
          Set(alice, bob),
          NonNegativeInt.tryCreate(2),
          s(999),
          testedProtocolVersion,
        )
      val view = TransactionView.tryCreate(hashOps)(
        viewCommonData,
        BlindedNode(rh(0)),
        TransactionSubviews.empty(testedProtocolVersion, hashOps),
        testedProtocolVersion,
      )
      val commonMetadata = CommonMetadata
        .create(hashOps, testedProtocolVersion)(
          ConfirmationPolicy.Signatory,
          domainId,
          MediatorRef(mediatorId),
          s(5417),
          new UUID(0, 0),
        )
        .value
      FullInformeeTree.tryCreate(
        GenTransactionTree.tryCreate(hashOps)(
          BlindedNode(rh(11)),
          commonMetadata,
          BlindedNode(rh(12)),
          MerkleSeq.fromSeq(hashOps, testedProtocolVersion)(view :: Nil),
        ),
        testedProtocolVersion,
      )
    }
    val informeeMessage = InformeeMessage(fullInformeeTree)(testedProtocolVersion)
    val mockTopologySnapshot = mock[TopologySnapshot]
    when(mockTopologySnapshot.consortiumThresholds(any[Set[LfPartyId]])).thenAnswer {
      (parties: Set[LfPartyId]) => Future.successful(parties.map(x => x -> PositiveInt.one).toMap)
    }
    val currentVersion =
      ResponseAggregation
        .fromRequest(
          requestId,
          informeeMessage,
          testedProtocolVersion,
          mockTopologySnapshot,
        )(anyTraceContext, executorService)
        .futureValue // without explicit ec it deadlocks on AnyTestSuite.serialExecutionContext

    def mediatorState: MediatorState = {
      val sut = new MediatorState(
        new InMemoryFinalizedResponseStore(loggerFactory),
        new InMemoryMediatorDeduplicationStore(loggerFactory, timeouts),
        mock[Clock],
        DomainTestMetrics.mediator,
        testedProtocolVersion,
        CachingConfigs.defaultFinalizedMediatorRequestsCache,
        timeouts,
        loggerFactory,
      )
      Await.result(sut.add(currentVersion), 1.second)
      sut
    }

    "fetching unfinalized items" should {
      val sut = mediatorState
      "respect the limit filter" in {
        sut.pendingRequestIdsBefore(CantonTimestamp.MinValue) shouldBe empty
        sut.pendingRequestIdsBefore(
          CantonTimestamp.MaxValue
        ) should contain only currentVersion.requestId
        Future.successful(succeed)
      }
      "have no more unfinalized after finalization" in {
        for {
          _ <- sut.replace(currentVersion, currentVersion.timeout(currentVersion.version)).value
        } yield {
          sut.pendingRequestIdsBefore(CantonTimestamp.MaxValue) shouldBe empty
        }
      }
    }

    "fetching items" should {
      "fetch only existing items" in {
        val sut = mediatorState
        for {
          progress <- sut.fetch(requestId).value
          noItem <- sut.fetch(RequestId(CantonTimestamp.MinValue)).value
        } yield {
          progress shouldBe Some(currentVersion)
          noItem shouldBe None
        }
      }
    }

    "updating items" should {
      val sut = mediatorState
      val newVersionTs = currentVersion.version.plusSeconds(1)
      val newVersion = currentVersion.withVersion(newVersionTs)

      // this should be handled by the processor that shouldn't be requesting the replacement
      "prevent updating to the same version" in {
        for {
          result <- loggerFactory.assertLogs(
            sut.replace(newVersion, newVersion).value,
            _.shouldBeCantonError(
              MediatorError.InternalError,
              _ shouldBe s"Request ${currentVersion.requestId} has an unexpected version ${currentVersion.requestId.unwrap} (expected version: ${newVersion.version}, new version: ${newVersion.version}).",
            ),
          )
        } yield result shouldBe None
      }

      "allow updating to a newer version" in {
        for {
          result <- sut.replace(currentVersion, newVersion).value
        } yield result shouldBe Some(())
      }
    }
  }
}
