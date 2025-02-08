// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.*
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.InformeeMessage
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.synchronizer.mediator.store.{
  InMemoryFinalizedResponseStore,
  InMemoryMediatorDeduplicationStore,
  MediatorState,
}
import com.digitalasset.canton.synchronizer.metrics.MediatorTestMetrics
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{
  ApplicationId,
  BaseTest,
  CommandId,
  FailOnShutdown,
  HasExecutionContext,
  LfPartyId,
}
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration
import java.util.UUID
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

class MediatorStateTest
    extends AsyncWordSpec
    with BaseTest
    with HasTestCloseContext
    with HasExecutionContext
    with FailOnShutdown { self =>

  "MediatorState" when {
    val requestId = RequestId(CantonTimestamp.Epoch)
    val fullInformeeTree = {
      val synchronizerId = DefaultTestIdentities.synchronizerId
      val participantId = DefaultTestIdentities.participant1
      val alice = LfPartyId.assertFromString("alice")
      val bob = LfPartyId.assertFromString("bob")
      val bobCp = Map(bob -> PositiveInt.tryCreate(2))
      val hashOps: HashOps = new SymbolicPureCrypto
      val h: Int => Hash = TestHash.digest
      val s: Int => Salt = TestSalt.generateSalt
      def rh(index: Int): RootHash = RootHash(h(index))
      val viewCommonData =
        ViewCommonData.tryCreate(hashOps)(
          ViewConfirmationParameters.tryCreate(
            Set(alice, bob),
            Seq(Quorum(bobCp, NonNegativeInt.tryCreate(2))),
          ),
          s(999),
          testedProtocolVersion,
        )
      val view = TransactionView.tryCreate(hashOps)(
        viewCommonData,
        BlindedNode(rh(0)),
        TransactionSubviews.empty(testedProtocolVersion, hashOps),
        testedProtocolVersion,
      )
      val submitterMetadata = SubmitterMetadata(
        NonEmpty(Set, alice),
        ApplicationId.assertFromString("kaese"),
        CommandId.assertFromString("wurst"),
        participantId,
        salt = s(6638),
        None,
        DeduplicationPeriod.DeduplicationDuration(Duration.ZERO),
        CantonTimestamp.MaxValue,
        None,
        hashOps,
        testedProtocolVersion,
      )
      val commonMetadata = CommonMetadata
        .create(hashOps, testedProtocolVersion)(
          synchronizerId,
          MediatorGroupRecipient(MediatorGroupIndex.zero),
          s(5417),
          new UUID(0, 0),
        )
      FullInformeeTree.tryCreate(
        GenTransactionTree.tryCreate(hashOps)(
          submitterMetadata,
          commonMetadata,
          BlindedNode(rh(12)),
          MerkleSeq.fromSeq(hashOps, testedProtocolVersion)(view :: Nil),
        ),
        testedProtocolVersion,
      )
    }
    val informeeMessage =
      InformeeMessage(fullInformeeTree, Signature.noSignature)(testedProtocolVersion)
    val mockTopologySnapshot = mock[TopologySnapshot]
    when(mockTopologySnapshot.consortiumThresholds(any[Set[LfPartyId]])(anyTraceContext))
      .thenAnswer { (parties: Set[LfPartyId]) =>
        FutureUnlessShutdown.pure(parties.map(x => x -> PositiveInt.one).toMap)
      }
    val currentVersion =
      ResponseAggregation
        .fromRequest(
          requestId,
          informeeMessage,
          requestId.unwrap.plusSeconds(300),
          requestId.unwrap.plusSeconds(600),
          mockTopologySnapshot,
        )(traceContext, executorService)
        .futureValueUS // without explicit ec it deadlocks on AnyTestSuite.serialExecutionContext

    def mediatorState: MediatorState = {
      val sut = new MediatorState(
        new InMemoryFinalizedResponseStore(loggerFactory),
        new InMemoryMediatorDeduplicationStore(loggerFactory, timeouts),
        mock[Clock],
        MediatorTestMetrics,
        testedProtocolVersion,
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
          _ <- sut.replace(currentVersion, currentVersion.timeout(currentVersion.version))
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
            sut.replace(newVersion, newVersion),
            _.shouldBeCantonError(
              MediatorError.InternalError,
              _ shouldBe s"Request ${currentVersion.requestId} has an unexpected version ${currentVersion.requestId.unwrap} (expected version: ${newVersion.version}, new version: ${newVersion.version}).",
            ),
          )
        } yield result shouldBe false
      }

      "allow updating to a newer version" in {
        for {
          result <- sut.replace(currentVersion, newVersion)
        } yield result shouldBe true
      }
    }
  }
}
