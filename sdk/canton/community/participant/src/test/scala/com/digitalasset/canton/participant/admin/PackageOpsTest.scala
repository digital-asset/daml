// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.Eval
import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{NonNegativeFiniteDuration, ProcessingTimeout}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.{InitialPageToken, ListVettedPackagesOpts, PageToken}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.participant.admin.PackageService.{DarDescription, DarMainPackageId}
import com.digitalasset.canton.participant.store.{
  ActiveContractStore,
  ContractStore,
  LogicalSyncPersistentState,
  PhysicalSyncPersistentState,
  SyncPersistentState,
}
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.participant.topology.{
  PackageOps,
  PackageOpsImpl,
  TopologyComponentFactory,
  TopologyManagerLookup,
}
import com.digitalasset.canton.store.{IndexedPhysicalSynchronizer, IndexedSynchronizer}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
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
  protected def buildSetup(includeSync2InStateManager: Boolean): T
  protected def sutName: String

  protected final def withTestSetup[R](test: T => R): R = test(buildSetup(false))
  protected final def withTestSetupSync2[R](test: T => R): R = test(buildSetup(true))

  s"$sutName.hasPackageVettingEntry" should {
    "return true" when {
      "one synchronizer topology snapshot has the package vetted" in withTestSetup { env =>
        import env.*
        unvettedPackagesForSnapshots(Set.empty)
        packageOps.hasVettedPackageEntry(pkgId1).failOnShutdown.map(_ shouldBe true)
      }
    }

    "return false" when {
      "one synchronizer topology snapshot has the package unvetted" in withTestSetup { env =>
        import env.*
        unvettedPackagesForSnapshots(Set(pkgId1))
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
          when(syncPersistentState.synchronizerIdx).thenReturn(indexedSynchronizer)

          packageOps.checkPackageUnused(pkgId1).leftOrFail("active contract with package id").map {
            err =>
              err.pkg shouldBe pkgId1
              err.contract shouldBe contractId
              err.synchronizerId shouldBe synchronizerId1.logical
          }
      }.failOnShutdown
    }
  }

  protected trait CommonTestSetup {
    def packageOps: PackageOps
    def includeSync2InStateManager: Boolean

    val stateManager = mock[SyncPersistentStateManager]
    val participantId1 = ParticipantId(UniqueIdentifier.tryCreate("participant", "one"))
    val participantId2 = ParticipantId(UniqueIdentifier.tryCreate("participant", "two"))

    private val anotherSynchronizerTopologySnapshot = mock[TopologySnapshot]

    val pkgId1 = LfPackageId.assertFromString("pkgId1")
    val pkgId2 = LfPackageId.assertFromString("pkgId2")
    val pkgId3 = LfPackageId.assertFromString("pkgId3")

    // Synchronizer names are intentially chosen such that sorting by identifier
    // then namespace is different from sorting by the string "identifier::namespace".
    val synchronizerId1 = SynchronizerId(
      UniqueIdentifier.tryCreate("synchronizerA", "one")
    ).toPhysical
    val synchronizerId2 = SynchronizerId(
      UniqueIdentifier.tryCreate("synchronizer1", "two")
    ).toPhysical
    val synchronizerId3 = SynchronizerId(
      UniqueIdentifier.tryCreate("synchronizer", "three")
    ).toPhysical

    val syncPersistentStateDummy = mock[SyncPersistentState]
    val logicalSyncPersistentStateDummy = mock[LogicalSyncPersistentState]

    val physicalSyncPersistentState = mock[PhysicalSyncPersistentState]
    val logicalSyncPersistentState = mock[LogicalSyncPersistentState]
    val syncPersistentState: SyncPersistentState =
      new SyncPersistentState(
        logicalSyncPersistentState,
        physicalSyncPersistentState,
        loggerFactory,
      )
    when(physicalSyncPersistentState.physicalSynchronizerIdx).thenReturn(
      IndexedPhysicalSynchronizer.tryCreate(synchronizerId1, index = 1)
    )

    when(stateManager.getAll).thenReturn(
      if (includeSync2InStateManager)
        Map(
          synchronizerId1 -> syncPersistentState,
          synchronizerId2 -> syncPersistentStateDummy,
          synchronizerId3 -> syncPersistentStateDummy,
        )
      else
        Map(synchronizerId1 -> syncPersistentState)
    )
    when(stateManager.getAllLogical).thenReturn(
      if (includeSync2InStateManager)
        Map(
          synchronizerId1.logical -> logicalSyncPersistentState,
          synchronizerId2.logical -> logicalSyncPersistentStateDummy,
          synchronizerId3.logical -> logicalSyncPersistentStateDummy,
        )
      else
        Map(synchronizerId1.logical -> logicalSyncPersistentState)
    )
    when(stateManager.getAllLatest).thenReturn(
      if (includeSync2InStateManager)
        Map(
          synchronizerId1.logical -> syncPersistentState,
          synchronizerId2.logical -> syncPersistentStateDummy,
          synchronizerId3.logical -> syncPersistentStateDummy,
        )
      else
        Map(synchronizerId1.logical -> syncPersistentState)
    )

    private val topologyComponentFactory = mock[TopologyComponentFactory]
    when(topologyComponentFactory.createHeadTopologySnapshot()(any[ExecutionContext]))
      .thenReturn(anotherSynchronizerTopologySnapshot)

    val contractStore = mock[ContractStore]

    when(stateManager.topologyFactoryFor(synchronizerId1))
      .thenReturn(Some(topologyComponentFactory))
    when(stateManager.topologyFactoryFor(synchronizerId2)).thenReturn(None)
    when(stateManager.contractStore).thenReturn(Eval.now(contractStore))

    val activeContractStore = mock[ActiveContractStore]

    when(logicalSyncPersistentState.activeContractStore).thenReturn(activeContractStore)
    when(activeContractStore.packageUsage(eqTo(pkgId1), eqTo(contractStore))(anyTraceContext))
      .thenReturn(FutureUnlessShutdown.pure(None))

    val mainPackageId = DarMainPackageId.tryCreate("darhash")

    def unvettedPackagesForSnapshots(
        unvettedForSynchronizerSnapshot: Set[LfPackageId]
    ): Unit =
      when(
        anotherSynchronizerTopologySnapshot.determinePackagesWithNoVettingEntry(
          participantId1,
          Set(pkgId1),
        )
      ).thenReturn(FutureUnlessShutdown.pure(unvettedForSynchronizerSnapshot))
  }
}

class PackageOpsTest extends PackageOpsTestBase {
  protected type T = TestSetup
  protected def buildSetup(includeSync2InStateManager: Boolean): T = new TestSetup(
    includeSync2InStateManager
  )
  protected def sutName: String = classOf[PackageOpsImpl].getSimpleName

  s"$sutName.vetPackages" should {
    "add a new vetted package" when {
      "it was not vetted" in withTestSetup { env =>
        import env.*

        arrangeCurrentlyVetted(List(pkgId1))
        expectNewVettingState(List(pkgId1, pkgId2))
        packageOps
          .vetPackages(Seq(pkgId1, pkgId2), PackageVettingSynchronization.NoSync, synchronizerId1)
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
          .vetPackages(Seq(pkgId1, pkgId2), PackageVettingSynchronization.NoSync, synchronizerId1)
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
            verify(topologyManagerSync1, never).proposeAndAuthorize(
              any[TopologyChangeOp],
              any[TopologyMapping],
              any[Option[PositiveInt]],
              any[Seq[Fingerprint]],
              any[ProtocolVersion],
              anyBoolean,
              any[ForceFlags],
              any[Option[NonNegativeFiniteDuration]],
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
        val str = String255.tryCreate("DAR descriptor")
        packageOps
          .revokeVettingForPackages(
            pkgId1,
            List(pkgId1, pkgId2),
            DarDescription(mainPackageId, str, str, str),
            synchronizerId1,
            ForceFlags.none,
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
        val str = String255.tryCreate("DAR descriptor")
        packageOps
          .revokeVettingForPackages(
            pkgId3,
            List(pkgId3),
            DarDescription(mainPackageId, str, str, str),
            synchronizerId1,
            ForceFlags.none,
          )
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
            verify(topologyManagerSync1, never).proposeAndAuthorize(
              any[TopologyChangeOp],
              any[TopologyMapping],
              any[Option[PositiveInt]],
              any[Seq[Fingerprint]],
              any[ProtocolVersion],
              anyBoolean,
              any[ForceFlags],
              any[Option[NonNegativeFiniteDuration]],
            )(anyTraceContext)

            succeed
          })
      }
    }
  }

  s"$sutName.getVettedPackages" should {
    "query synchronizers in the correct order" in withTestSetupSync2 { env =>
      import env.*

      arrangeCurrentlyVetted(List(pkgId1))
      packageOps
        .getVettedPackages(
          ListVettedPackagesOpts(None, None, InitialPageToken, PositiveInt.tryCreate(100))
        )
        .value
        .unwrap
        .map(inside(_) { case UnlessShutdown.Outcome(Right(vettedPackages)) =>
          vettedPackages should have length 6
          vettedPackages.sorted(PageToken.orderingVettedPackages) should equal(
            vettedPackages
          )
          vettedPackages.sortBy(vp =>
            vp.synchronizerId.toProtoPrimitive -> vp.participantId.toProtoPrimitive
          ) should not equal vettedPackages
        })
    }
  }

  protected class TestSetup(override val includeSync2InStateManager: Boolean)
      extends CommonTestSetup {
    val topologyManagerSync1 = mock[SynchronizerTopologyManager]
    when(topologyManagerSync1.psid).thenReturn(synchronizerId1)
    val topologyManagerSync2 = mock[SynchronizerTopologyManager]
    when(topologyManagerSync2.psid).thenReturn(synchronizerId2)
    val topologyManagerSync3 = mock[SynchronizerTopologyManager]
    when(topologyManagerSync3.psid).thenReturn(synchronizerId3)

    for {
      topologyManager <- Seq(topologyManagerSync1, topologyManagerSync2, topologyManagerSync3)
    } yield {
      val topologyStore = mock[TopologyStore[SynchronizerStore]]
      when(topologyManager.store).thenReturn(topologyStore)
    }

    val packageOps = new PackageOpsImpl(
      participantId = participantId1,
      stateManager = stateManager,
      topologyManagerLookup = new TopologyManagerLookup(
        lookupByPsid = { psid =>
          if (psid == topologyManagerSync1.psid) Some(topologyManagerSync1)
          else if (psid == topologyManagerSync2.psid) Some(topologyManagerSync2)
          else if (psid == topologyManagerSync3.psid) Some(topologyManagerSync3)
          else None
        },
        lookupActivePsidByLsid = { lsid =>
          if (lsid == topologyManagerSync1.psid.logical) Some(topologyManagerSync1.psid)
          else if (lsid == topologyManagerSync2.psid.logical) Some(topologyManagerSync2.psid)
          else if (lsid == topologyManagerSync3.psid.logical) Some(topologyManagerSync3.psid)
          else None
        },
      ),
      initialProtocolVersion = testedProtocolVersion,
      loggerFactory = loggerFactory,
      timeouts = ProcessingTimeout(),
      futureSupervisor = futureSupervisor,
    )

    def arrangeCurrentlyVetted(currentlyVettedPackages: List[LfPackageId]) =
      for {
        topologyManager <- Seq(topologyManagerSync1, topologyManagerSync2, topologyManagerSync3)
      } yield {
        when(
          topologyManager.store.findPositiveTransactions(
            eqTo(CantonTimestamp.MaxValue),
            eqTo(true),
            eqTo(false),
            eqTo(Seq(VettedPackages.code)),
            eqTo(Some(NonEmpty(Seq, participantId1.uid))),
            eqTo(None),
            any[Option[(Option[UniqueIdentifier], Int)]],
          )(anyTraceContext)
        ).thenReturn(
          FutureUnlessShutdown.pure(
            packagesVettedStoredTx(currentlyVettedPackages, Seq(participantId1))
          )
        )

        when(
          topologyManager.store.findPositiveTransactions(
            eqTo(CantonTimestamp.MaxValue),
            eqTo(true),
            eqTo(false),
            eqTo(Seq(VettedPackages.code)),
            eqTo(None),
            eqTo(None),
            any[Option[(Option[UniqueIdentifier], Int)]],
          )(anyTraceContext)
        ).thenReturn(
          FutureUnlessShutdown.pure(
            packagesVettedStoredTx(currentlyVettedPackages, Seq(participantId1, participantId2))
          )
        )
      }

    val txSerial = PositiveInt.tryCreate(1)

    def expectNewVettingState(newVettedPackagesState: List[LfPackageId]) =
      when(
        topologyManagerSync1.proposeAndAuthorize(
          eqTo(TopologyChangeOp.Replace),
          eqTo(
            VettedPackages.tryCreate(
              participantId1,
              VettedPackage.unbounded(newVettedPackagesState),
            )
          ),
          eqTo(Some(txSerial.tryAdd(1))),
          eqTo(Seq.empty),
          eqTo(testedProtocolVersion),
          eqTo(true),
          eqTo(ForceFlags.none),
          any[Option[NonNegativeFiniteDuration]],
        )(anyTraceContext)
      ).thenReturn(
        EitherT.rightT(signedTopologyTransaction(List(pkgId2)))
      )

    def packagesVettedStoredTx(
        vettedPackages: List[LfPackageId],
        participantIds: Seq[ParticipantId],
    ) =
      StoredTopologyTransactions(
        participantIds.map(participantId =>
          StoredTopologyTransaction(
            sequenced = SequencedTime(CantonTimestamp.MaxValue),
            validFrom = EffectiveTime(CantonTimestamp.MinValue),
            validUntil = None,
            transaction = signedTopologyTransaction(vettedPackages, participantId),
            rejectionReason = None,
          )
        )
      )

    private def signedTopologyTransaction(
        vettedPackages: List[LfPackageId],
        participantId: ParticipantId = participantId1,
    ) =
      SignedTopologyTransaction.withSignatures(
        transaction = TopologyTransaction(
          op = TopologyChangeOp.Replace,
          serial = txSerial,
          mapping =
            VettedPackages.tryCreate(participantId, VettedPackage.unbounded(vettedPackages)),
          protocolVersion = testedProtocolVersion,
        ),
        signatures = Signature.noSignatures,
        isProposal = false,
        testedProtocolVersion,
      )
  }
}
