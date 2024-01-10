// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Fingerprint, Hash, HashAlgorithm, HashPurpose, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageMissingDependencies
import com.digitalasset.canton.participant.admin.PackageService.DarDescriptor
import com.digitalasset.canton.participant.store.{
  ActiveContractStore,
  ContractStore,
  SyncDomainPersistentState,
}
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager
import com.digitalasset.canton.participant.topology.{
  ParticipantTopologyManager,
  TopologyComponentFactory,
}
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactionX,
  StoredTopologyTransactionsX,
  TopologyStoreX,
}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.{Add, Remove}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{
  AuthorizedTopologyManagerX,
  DomainId,
  ParticipantId,
  UniqueIdentifier,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, LfPackageId}
import org.mockito.ArgumentMatchersSugar
import org.mockito.captor.ArgCaptor
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}

trait PackageOpsTestBase extends AsyncWordSpec with BaseTest with ArgumentMatchersSugar {
  protected type T <: CommonTestSetup
  protected def buildSetup: T
  protected def sutName: String

  protected final def withTestSetup[R](test: T => R): R = test(buildSetup)

  s"$sutName.isPackageVetted" should {
    "return true" when {
      "head authorized store has the package vetted" in withTestSetup { env =>
        import env.*
        unvettedPackagesForSnapshots(Set.empty, Set(pkgId1))
        packageOps.isPackageVetted(pkgId1).map(_ shouldBe true)
      }

      "one domain topology snapshot has the package vetted" in withTestSetup { env =>
        import env.*
        unvettedPackagesForSnapshots(Set(pkgId1), Set.empty)
        packageOps.isPackageVetted(pkgId1).map(_ shouldBe true)
      }

      "all topology snapshots have the package vetted" in withTestSetup { env =>
        import env.*
        unvettedPackagesForSnapshots(Set.empty, Set.empty)
        packageOps.isPackageVetted(pkgId1).map(_ shouldBe true)
      }
    }

    "return false" when {
      "all topology snapshots have the package unvetted" in withTestSetup { env =>
        import env.*
        unvettedPackagesForSnapshots(Set(pkgId1), Set(pkgId1))
        packageOps.isPackageVetted(pkgId1).map(_ shouldBe false)
      }
    }

    "return a Left with an error" when {
      "a relevant package is missing locally" in withTestSetup { env =>
        import env.*
        when(
          headAuthorizedTopologySnapshot.findUnvettedPackagesOrDependencies(
            participantId,
            Set(pkgId1),
          )
        ).thenReturn(EitherT.rightT(Set.empty))
        when(
          anotherDomainTopologySnapshot.findUnvettedPackagesOrDependencies(
            participantId,
            Set(pkgId1),
          )
        ).thenReturn(EitherT.leftT(missingPkgId))

        packageOps
          .isPackageVetted(pkgId1)
          .leftOrFail("missing package id")
          .map(_ shouldBe PackageMissingDependencies.Reject(pkgId1, missingPkgId))
      }
    }
  }

  s"$sutName.checkPackageUnused" should {
    "return a Right" when {
      "all domain states report no active contracts for package id" in withTestSetup { env =>
        import env.*

        packageOps.checkPackageUnused(pkgId1).map(_ shouldBe ())
      }
    }

    "return a Left with the used package" when {
      "a domain state reports an active contract with the package id" in withTestSetup { env =>
        import env.*
        val contractId = TransactionBuilder.newCid
        when(activeContractStore.packageUsage(eqTo(pkgId1), eqTo(contractStore))(anyTraceContext))
          .thenReturn(Future.successful(Some(contractId)))
        val indexedDomain = IndexedDomain.tryCreate(domainId1, 1)
        when(syncDomainPersistentState.domainId).thenReturn(indexedDomain)

        packageOps.checkPackageUnused(pkgId1).leftOrFail("active contract with package id").map {
          err =>
            err.pkg shouldBe pkgId1
            err.contract shouldBe contractId
            err.domain shouldBe domainId1
        }
      }
    }
  }

  protected trait CommonTestSetup {
    def packageOps: PackageOps

    val stateManager = mock[SyncDomainPersistentStateManager]
    val participantId = ParticipantId(UniqueIdentifier.tryCreate("participant", "one"))

    val headAuthorizedTopologySnapshot = mock[TopologySnapshot]
    val anotherDomainTopologySnapshot = mock[TopologySnapshot]

    val pkgId1 = LfPackageId.assertFromString("pkgId1")
    val pkgId2 = LfPackageId.assertFromString("pkgId2")
    val pkgId3 = LfPackageId.assertFromString("pkgId3")

    val packagesToBeVetted = Seq(pkgId1, pkgId2)
    val packagesToBeUnvetted = List(pkgId1, pkgId2)

    val missingPkgId = LfPackageId.assertFromString("missing")
    val domainId1 = DomainId(UniqueIdentifier.tryCreate("domain", "one"))
    val domainId2 = DomainId(UniqueIdentifier.tryCreate("domain", "two"))

    val syncDomainPersistentState: SyncDomainPersistentState = mock[SyncDomainPersistentState]
    when(stateManager.getAll).thenReturn(Map(domainId1 -> syncDomainPersistentState))
    val topologyComponentFactory = mock[TopologyComponentFactory]
    when(topologyComponentFactory.createHeadTopologySnapshot()(any[ExecutionContext]))
      .thenReturn(anotherDomainTopologySnapshot)

    when(stateManager.topologyFactoryFor(domainId1)).thenReturn(Some(topologyComponentFactory))
    when(stateManager.topologyFactoryFor(domainId2)).thenReturn(None)

    val activeContractStore = mock[ActiveContractStore]
    val contractStore = mock[ContractStore]
    when(syncDomainPersistentState.activeContractStore).thenReturn(activeContractStore)
    when(syncDomainPersistentState.contractStore).thenReturn(contractStore)
    when(activeContractStore.packageUsage(eqTo(pkgId1), eqTo(contractStore))(anyTraceContext))
      .thenReturn(Future.successful(None))

    val hash = Hash
      .build(HashPurpose.TopologyTransactionSignature, HashAlgorithm.Sha256)
      .add("darhash")
      .finish()

    def unvettedPackagesForSnapshots(
        unvettedForAuthorizedSnapshot: Set[LfPackageId],
        unvettedForDomainSnapshot: Set[LfPackageId],
    ): Unit = {
      when(
        headAuthorizedTopologySnapshot.findUnvettedPackagesOrDependencies(
          participantId,
          Set(pkgId1),
        )
      ).thenReturn(EitherT.rightT(unvettedForAuthorizedSnapshot))
      when(
        anotherDomainTopologySnapshot.findUnvettedPackagesOrDependencies(
          participantId,
          Set(pkgId1),
        )
      ).thenReturn(EitherT.rightT(unvettedForDomainSnapshot))
    }
  }
}

class PackageOpsTest extends PackageOpsTestBase {
  protected type T = TestSetup
  protected def buildSetup: T = new TestSetup()
  protected def sutName: String = classOf[PackageOpsImpl].getSimpleName

  s"$sutName.vetPackages" should {
    "vet the requested packages" when {
      "if some of them are unvetted" in withTestSetup { env =>
        import env.*
        val topologyStateUpdateArgCaptor = ArgCaptor[TopologyStateUpdate[Add]]
        when(
          topologyManager.authorize(
            topologyStateUpdateArgCaptor.capture,
            eqTo(None),
            eqTo(testedProtocolVersion),
            eqTo(false),
            eqTo(false),
          )(anyTraceContext)
        )
          .thenReturn(EitherT.rightT(mock[SignedTopologyTransaction[Nothing]]))

        packageOps
          .vetPackages(packagesToBeVetted, synchronize = false)
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
            inside(topologyStateUpdateArgCaptor.value.element.mapping) {
              case VettedPackages(`participantId`, `packagesToBeVetted`) => succeed
            }
          })
      }
    }

    "not vet the packages" when {
      "when all of them are already vetted" in withTestSetup { env =>
        import env.*
        when(
          topologyManager
            .unvettedPackages(eqTo(participantId), eqTo(packagesToBeVetted.toSet))(anyTraceContext)
        ).thenReturn(Future.successful(Set.empty))

        packageOps
          .vetPackages(packagesToBeVetted, synchronize = false)
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
            verify(topologyManager).unvettedPackages(any[ParticipantId], any[Set[LfPackageId]])(
              anyTraceContext
            )
            verifyNoMoreInteractions(topologyManager)
            succeed
          })
      }
    }
  }

  s"$sutName.revokeVettingForPackages" should {
    "create a vetting revocation transaction and authorize it with the topology manager" in withTestSetup {
      env =>
        import env.*
        packageOps
          .revokeVettingForPackages(
            mainPkg = pkgId1,
            packages = packagesToBeUnvetted,
            darDescriptor = DarDescriptor(hash, String255.tryCreate("darname")),
          )
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
            verify(topologyManager).authorize(
              eqTo(revocationTxMock),
              eqTo(None),
              eqTo(testedProtocolVersion),
              eqTo(true),
              eqTo(false),
            )(anyTraceContext)

            succeed
          })
    }
  }

  protected class TestSetup extends CommonTestSetup {
    val topologyManager = mock[ParticipantTopologyManager]

    val packageOps = new PackageOpsImpl(
      participantId,
      headAuthorizedTopologySnapshot,
      stateManager,
      topologyManager,
      testedProtocolVersion,
      loggerFactory,
    )

    when(
      topologyManager
        .unvettedPackages(eqTo(participantId), eqTo(packagesToBeVetted.toSet))(anyTraceContext)
    )
      .thenReturn(Future.successful(packagesToBeVetted.toSet))

    val revocationTxMock = mock[TopologyTransaction[Remove]]
    when(
      topologyManager.genTransaction(
        eqTo(TopologyChangeOp.Remove),
        eqTo(VettedPackages(participantId, packagesToBeUnvetted)),
        eqTo(testedProtocolVersion),
      )(anyTraceContext)
    ).thenReturn(EitherT.rightT(revocationTxMock))

    when(
      topologyManager.authorize(
        eqTo(revocationTxMock),
        eqTo(None),
        eqTo(testedProtocolVersion),
        eqTo(true),
        eqTo(false),
      )(anyTraceContext)
    ).thenReturn(EitherT.rightT(mock[SignedTopologyTransaction[Nothing]]))
  }
}

class PackageOpsXTest extends PackageOpsTestBase {
  protected type T = TestSetupX
  protected def buildSetup: T = new TestSetupX()
  protected def sutName: String = classOf[PackageOpsX].getSimpleName

  s"$sutName.vetPackages" should {
    "add a new vetted package" when {
      "it was not vetted" in withTestSetup { env =>
        import env.*

        arrangeCurrentlyVetted(List(pkgId1))
        expectNewVettingState(List(pkgId1, pkgId2))
        packageOps
          .vetPackages(Seq(pkgId1, pkgId2), synchronize = false)
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
          .vetPackages(Seq(pkgId1, pkgId2), synchronize = false)
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
            verify(topologyManagerX, never).proposeAndAuthorize(
              any[TopologyChangeOpX],
              any[TopologyMappingX],
              any[Option[PositiveInt]],
              any[Seq[Fingerprint]],
              any[ProtocolVersion],
              anyBoolean,
              anyBoolean,
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
            verify(topologyManagerX, never).proposeAndAuthorize(
              any[TopologyChangeOpX],
              any[TopologyMappingX],
              any[Option[PositiveInt]],
              any[Seq[Fingerprint]],
              any[ProtocolVersion],
              anyBoolean,
              anyBoolean,
            )(anyTraceContext)

            succeed
          })
      }
    }
  }

  protected class TestSetupX extends CommonTestSetup {
    val topologyManagerX = mock[AuthorizedTopologyManagerX]

    private val nodeId: UniqueIdentifier = UniqueIdentifier.tryCreate("node", "one")
    val packageOps = new PackageOpsX(
      participantId = participantId,
      headAuthorizedTopologySnapshot = headAuthorizedTopologySnapshot,
      manager = stateManager,
      topologyManager = topologyManagerX,
      nodeId = nodeId,
      initialProtocolVersion = testedProtocolVersion,
      loggerFactory = loggerFactory,
      timeouts = ProcessingTimeout(),
    )

    val topologyStoreX = mock[TopologyStoreX[AuthorizedStore]]
    when(topologyManagerX.store).thenReturn(topologyStoreX)
    val txSerial = PositiveInt.tryCreate(1)
    def arrangeCurrentlyVetted(currentlyVettedPackages: List[LfPackageId]) =
      when(
        topologyStoreX.findPositiveTransactions(
          eqTo(CantonTimestamp.MaxValue),
          eqTo(true),
          eqTo(false),
          eqTo(Seq(VettedPackagesX.code)),
          eqTo(Some(Seq(nodeId))),
          eqTo(None),
        )(anyTraceContext)
      ).thenReturn(Future.successful(packagesVettedStoredTx(currentlyVettedPackages)))

    def expectNewVettingState(newVettedPackagesState: List[LfPackageId]) =
      when(
        topologyManagerX.proposeAndAuthorize(
          eqTo(TopologyChangeOpX.Replace),
          eqTo(VettedPackagesX(participantId, None, newVettedPackagesState)),
          eqTo(Some(txSerial.tryAdd(1))),
          eqTo(Seq(participantId.uid.namespace.fingerprint)),
          eqTo(testedProtocolVersion),
          eqTo(true),
          eqTo(false),
        )(anyTraceContext)
      ).thenReturn(EitherT.rightT(signedTopologyTransactionX(List(pkgId2))))

    def packagesVettedStoredTx(vettedPackages: List[LfPackageId]) =
      StoredTopologyTransactionsX(
        Seq(
          StoredTopologyTransactionX(
            sequenced = SequencedTime(CantonTimestamp.MaxValue),
            validFrom = EffectiveTime(CantonTimestamp.MinValue),
            validUntil = None,
            transaction = signedTopologyTransactionX(vettedPackages),
          )
        )
      )

    private def signedTopologyTransactionX(vettedPackages: List[LfPackageId]) =
      SignedTopologyTransactionX(
        transaction = TopologyTransactionX(
          op = TopologyChangeOpX.Replace,
          serial = txSerial,
          mapping = VettedPackagesX(participantId, None, vettedPackages),
          protocolVersion = testedProtocolVersion,
        ),
        signatures = NonEmpty(Set, Signature.noSignature),
        isProposal = false,
      )(
        SignedTopologyTransactionX.supportedProtoVersions.protocolVersionRepresentativeFor(
          testedProtocolVersion
        )
      )
  }
}
