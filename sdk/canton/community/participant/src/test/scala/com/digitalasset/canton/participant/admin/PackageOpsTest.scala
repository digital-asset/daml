// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.Eval
import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.participant.admin.PackageService.DarDescriptor
import com.digitalasset.canton.participant.store.{
  ActiveContractStore,
  ContractStore,
  SyncPersistentState,
}
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.participant.topology.{
  PackageOps,
  PackageOpsImpl,
  TopologyComponentFactory,
}
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TopologyStore,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, LfPackageId}
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import org.mockito.ArgumentMatchersSugar
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext

trait PackageOpsTestBase extends AsyncWordSpec with BaseTest with ArgumentMatchersSugar {
  protected type T <: CommonTestSetup
  protected def buildSetup: T
  protected def sutName: String

  protected final def withTestSetup[R](test: T => R): R = test(buildSetup)

  s"$sutName.hasPackageVettingEntry" should {
    "return true" when {
      "head authorized store has the package vetted" in withTestSetup { env =>
        import env.*
        unvettedPackagesForSnapshots(Set.empty, Set(pkgId1))
        packageOps.hasVettedPackageEntry(pkgId1).failOnShutdown.map(_ shouldBe true)
      }

      "one synchronizer topology snapshot has the package vetted" in withTestSetup { env =>
        import env.*
        unvettedPackagesForSnapshots(Set(pkgId1), Set.empty)
        packageOps.hasVettedPackageEntry(pkgId1).failOnShutdown.map(_ shouldBe true)
      }

      "all topology snapshots have the package vetted" in withTestSetup { env =>
        import env.*
        unvettedPackagesForSnapshots(Set.empty, Set.empty)
        packageOps.hasVettedPackageEntry(pkgId1).failOnShutdown.map(_ shouldBe true)
      }
    }

    "return false" when {
      "all topology snapshots have the package unvetted" in withTestSetup { env =>
        import env.*
        unvettedPackagesForSnapshots(Set(pkgId1), Set(pkgId1))
        packageOps.hasVettedPackageEntry(pkgId1).failOnShutdown.map(_ shouldBe false)
      }
    }
  }

  s"$sutName.checkPackageUnused" should {
    "return a Right" when {
      "all synchronizer states report no active contracts for package id" in withTestSetup { env =>
        import env.*

        packageOps.checkPackageUnused(pkgId1).map(_ shouldBe ())
      }.failOnShutdown
    }

    "return a Left with the used package" when {
      "a synchronizer state reports an active contract with the package id" in withTestSetup {
        env =>
          import env.*
          val contractId = TransactionBuilder.newCid
          when(activeContractStore.packageUsage(eqTo(pkgId1), eqTo(contractStore))(anyTraceContext))
            .thenReturn(FutureUnlessShutdown.pure(Some(contractId)))
          val indexedSynchronizer = IndexedSynchronizer.tryCreate(synchronizerId1, 1)
          when(syncPersistentState.indexedSynchronizer).thenReturn(indexedSynchronizer)

          packageOps.checkPackageUnused(pkgId1).leftOrFail("active contract with package id").map {
            err =>
              err.pkg shouldBe pkgId1
              err.contract shouldBe contractId
              err.synchronizerId shouldBe synchronizerId1
          }
      }.failOnShutdown
    }
  }

  protected trait CommonTestSetup {
    def packageOps: PackageOps

    val stateManager = mock[SyncPersistentStateManager]
    val participantId = ParticipantId(UniqueIdentifier.tryCreate("participant", "one"))

    val headAuthorizedTopologySnapshot = mock[TopologySnapshot]
    val anotherSynchronizerTopologySnapshot = mock[TopologySnapshot]

    val pkgId1 = LfPackageId.assertFromString("pkgId1")
    val pkgId2 = LfPackageId.assertFromString("pkgId2")
    val pkgId3 = LfPackageId.assertFromString("pkgId3")

    val packagesToBeVetted = Seq(pkgId1, pkgId2)
    val packagesToBeUnvetted = List(pkgId1, pkgId2)

    val missingPkgId = LfPackageId.assertFromString("missing")
    val synchronizerId1 = SynchronizerId(UniqueIdentifier.tryCreate("synchronizer", "one"))
    val synchronizerId2 = SynchronizerId(UniqueIdentifier.tryCreate("synchronizer", "two"))

    val syncPersistentState: SyncPersistentState = mock[SyncPersistentState]
    when(stateManager.getAll).thenReturn(Map(synchronizerId1 -> syncPersistentState))
    val topologyComponentFactory = mock[TopologyComponentFactory]
    when(topologyComponentFactory.createHeadTopologySnapshot()(any[ExecutionContext]))
      .thenReturn(anotherSynchronizerTopologySnapshot)

    val contractStore = mock[ContractStore]

    when(stateManager.topologyFactoryFor(synchronizerId1, testedProtocolVersion))
      .thenReturn(Some(topologyComponentFactory))
    when(stateManager.topologyFactoryFor(synchronizerId2, testedProtocolVersion)).thenReturn(None)
    when(stateManager.contractStore).thenReturn(Eval.now(contractStore))

    val activeContractStore = mock[ActiveContractStore]

    when(syncPersistentState.activeContractStore).thenReturn(activeContractStore)
    when(activeContractStore.packageUsage(eqTo(pkgId1), eqTo(contractStore))(anyTraceContext))
      .thenReturn(FutureUnlessShutdown.pure(None))

    val hash = Hash
      .build(HashPurpose.TopologyTransactionSignature, HashAlgorithm.Sha256)
      .add("darhash")
      .finish()

    def unvettedPackagesForSnapshots(
        unvettedForAuthorizedSnapshot: Set[LfPackageId],
        unvettedForSynchronizerSnapshot: Set[LfPackageId],
    ): Unit = {
      when(
        headAuthorizedTopologySnapshot.determinePackagesWithNoVettingEntry(
          participantId,
          Set(pkgId1),
        )
      ).thenReturn(FutureUnlessShutdown.pure(unvettedForAuthorizedSnapshot))
      when(
        anotherSynchronizerTopologySnapshot.determinePackagesWithNoVettingEntry(
          participantId,
          Set(pkgId1),
        )
      ).thenReturn(FutureUnlessShutdown.pure(unvettedForSynchronizerSnapshot))
    }
  }
}

class PackageOpsTest extends PackageOpsTestBase {
  protected type T = TestSetup
  protected def buildSetup: T = new TestSetup()
  protected def sutName: String = classOf[PackageOpsImpl].getSimpleName

  s"$sutName.vetPackages" should {
    "add a new vetted package" when {
      "it was not vetted" in withTestSetup { env =>
        import env.*

        arrangeCurrentlyVetted(List(pkgId1))
        expectNewVettingState(List(pkgId1, pkgId2))
        packageOps
          .vetPackages(Seq(pkgId1, pkgId2), PackageVettingSynchronization.NoSync)
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) => succeed })
      }
    }

    "not authorize a topology change" when {
      "the vetted package set is unchanged" in withTestSetup { env =>
        import env.*

        // Not ordered to prove that we check set-equality not ordered
        arrangeCurrentlyVetted(List(pkgId2, pkgId1))
        packageOps
          .vetPackages(Seq(pkgId1, pkgId2), PackageVettingSynchronization.NoSync)
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
            verify(topologyManager, never).proposeAndAuthorize(
              any[TopologyChangeOp],
              any[TopologyMapping],
              any[Option[PositiveInt]],
              any[Seq[Fingerprint]],
              any[ProtocolVersion],
              anyBoolean,
              any[ForceFlags],
            )(anyTraceContext)

            succeed
          })
      }
    }
  }

  s"$sutName.revokeVettingForPackages" should {
    "revoke vetting for packages" when {
      "they were vetted" in withTestSetup { env =>
        import env.*

        arrangeCurrentlyVetted(List(pkgId1, pkgId2, pkgId3))
        expectNewVettingState(List(pkgId3))
        packageOps
          .revokeVettingForPackages(
            pkgId1,
            List(pkgId1, pkgId2),
            DarDescriptor(hash, String255.tryCreate("DAR descriptor")),
          )
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) => succeed })
      }
    }

    "not authorize a topology change" when {
      "the vetted package set is unchanged" in withTestSetup { env =>
        import env.*

        // Not ordered to prove that we check set-equality not ordered
        arrangeCurrentlyVetted(List(pkgId2, pkgId1))
        packageOps
          .revokeVettingForPackages(
            pkgId3,
            List(pkgId3),
            DarDescriptor(hash, String255.tryCreate("DAR descriptor")),
          )
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
            verify(topologyManager, never).proposeAndAuthorize(
              any[TopologyChangeOp],
              any[TopologyMapping],
              any[Option[PositiveInt]],
              any[Seq[Fingerprint]],
              any[ProtocolVersion],
              anyBoolean,
              any[ForceFlags],
            )(anyTraceContext)

            succeed
          })
      }
    }
  }

  protected class TestSetup extends CommonTestSetup {
    val topologyManager = mock[AuthorizedTopologyManager]

    private val nodeId: UniqueIdentifier = UniqueIdentifier.tryCreate("node", "one")
    val packageOps = new PackageOpsImpl(
      participantId = participantId,
      headAuthorizedTopologySnapshot = headAuthorizedTopologySnapshot,
      stateManager = stateManager,
      topologyManager = topologyManager,
      nodeId = nodeId,
      initialProtocolVersion = testedProtocolVersion,
      loggerFactory = loggerFactory,
      timeouts = ProcessingTimeout(),
      futureSupervisor = futureSupervisor,
    )

    val topologyStore = mock[TopologyStore[AuthorizedStore]]
    when(topologyManager.store).thenReturn(topologyStore)
    val txSerial = PositiveInt.tryCreate(1)
    def arrangeCurrentlyVetted(currentlyVettedPackages: List[LfPackageId]) =
      when(
        topologyStore.findPositiveTransactions(
          eqTo(CantonTimestamp.MaxValue),
          eqTo(true),
          eqTo(false),
          eqTo(Seq(VettedPackages.code)),
          eqTo(Some(Seq(nodeId))),
          eqTo(None),
        )(anyTraceContext)
      ).thenReturn(FutureUnlessShutdown.pure(packagesVettedStoredTx(currentlyVettedPackages)))

    def expectNewVettingState(newVettedPackagesState: List[LfPackageId]) =
      when(
        topologyManager.proposeAndAuthorize(
          eqTo(TopologyChangeOp.Replace),
          eqTo(
            VettedPackages.tryCreate(
              participantId,
              VettedPackage.unbounded(newVettedPackagesState),
            )
          ),
          eqTo(Some(txSerial.tryAdd(1))),
          eqTo(Seq.empty),
          eqTo(testedProtocolVersion),
          eqTo(true),
          eqTo(ForceFlags(ForceFlag.AllowUnvetPackage)),
        )(anyTraceContext)
      ).thenReturn(EitherT.rightT(signedTopologyTransaction(List(pkgId2))))

    def packagesVettedStoredTx(vettedPackages: List[LfPackageId]) =
      StoredTopologyTransactions(
        Seq(
          StoredTopologyTransaction(
            sequenced = SequencedTime(CantonTimestamp.MaxValue),
            validFrom = EffectiveTime(CantonTimestamp.MinValue),
            validUntil = None,
            transaction = signedTopologyTransaction(vettedPackages),
            rejectionReason = None,
          )
        )
      )

    private def signedTopologyTransaction(vettedPackages: List[LfPackageId]) =
      SignedTopologyTransaction(
        transaction = TopologyTransaction(
          op = TopologyChangeOp.Replace,
          serial = txSerial,
          mapping =
            VettedPackages.tryCreate(participantId, VettedPackage.unbounded(vettedPackages)),
          protocolVersion = testedProtocolVersion,
        ),
        signatures = NonEmpty(Set, Signature.noSignature),
        isProposal = false,
      )(
        SignedTopologyTransaction.supportedProtoVersions.protocolVersionRepresentativeFor(
          testedProtocolVersion
        )
      )
  }
}
