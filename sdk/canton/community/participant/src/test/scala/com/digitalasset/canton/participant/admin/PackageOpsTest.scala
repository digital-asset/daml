// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import com.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageMissingDependencies
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
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.{Add, Remove}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
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

  s"$sutName.isPackageVettedOrCheckOnly" should {
    "return true" when {
      "head authorized store has the package vetted" in withTestSetup { env =>
        import env.*
        unvettedPackagesForSnapshots(Set.empty, Set(pkgId1))
        checkOnlyPackagesForSnapshots(Set.empty, Set(pkgId1))
        packageOps.isPackageKnown(pkgId1).failOnShutdown.map(_ shouldBe true)
      }

      "one domain topology snapshot has the package vetted" in withTestSetup { env =>
        import env.*
        unvettedPackagesForSnapshots(Set(pkgId1), Set.empty)
        checkOnlyPackagesForSnapshots(Set(pkgId1), Set.empty)
        packageOps.isPackageKnown(pkgId1).failOnShutdown.map(_ shouldBe true)
      }

      "all topology snapshots have the package vetted" in withTestSetup { env =>
        import env.*
        unvettedPackagesForSnapshots(Set.empty, Set.empty)
        checkOnlyPackagesForSnapshots(Set.empty, Set.empty)
        packageOps.isPackageKnown(pkgId1).failOnShutdown.map(_ shouldBe true)
      }
    }

    "return false" when {
      "all topology snapshots have the package not vetted nor check-only" in withTestSetup { env =>
        import env.*
        unvettedPackagesForSnapshots(Set(pkgId1), Set(pkgId1))
        checkOnlyPackagesForSnapshots(Set(pkgId1), Set(pkgId1))
        packageOps.isPackageKnown(pkgId1).failOnShutdown.map(_ shouldBe false)
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
          headAuthorizedTopologySnapshot.findPackagesOrDependenciesNotDeclaredAsCheckOnly(
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
        when(
          anotherDomainTopologySnapshot.findPackagesOrDependenciesNotDeclaredAsCheckOnly(
            participantId,
            Set(pkgId1),
          )
        ).thenReturn(EitherT.rightT(Set.empty))

        packageOps
          .isPackageKnown(pkgId1)
          .leftOrFail("missing package id")
          .failOnShutdown
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

    val depPkgs = List(pkgId2, pkgId3)
    val packages = pkgId1 :: depPkgs

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
    def checkOnlyPackagesForSnapshots(
        checkOnlyForAuthorizedSnapshot: Set[LfPackageId],
        checkOnlyForDomainSnapshot: Set[LfPackageId],
    ): Unit = {
      when(
        headAuthorizedTopologySnapshot.findPackagesOrDependenciesNotDeclaredAsCheckOnly(
          participantId,
          Set(pkgId1),
        )
      ).thenReturn(EitherT.rightT(checkOnlyForAuthorizedSnapshot))
      when(
        anotherDomainTopologySnapshot.findPackagesOrDependenciesNotDeclaredAsCheckOnly(
          participantId,
          Set(pkgId1),
        )
      ).thenReturn(EitherT.rightT(checkOnlyForDomainSnapshot))
    }
  }
}

// TODO(#21671): Unit test synchronization of enable and disable once state is final
class PackageOpsTest extends PackageOpsTestBase {
  protected type T = TestSetup
  protected def buildSetup: T = new TestSetup()
  protected def sutName: String = classOf[PackageOpsImpl].getSimpleName

  s"$sutName.enableDarPackages" should {
    "mark all DAR packages as vetted" when {
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

        arrange(
          notVetted = Seq(packages.toSet -> Set(pkgId2)),
          existingMappings = Seq(CheckOnlyPackages(participantId, packages) -> false),
        )

        packageOps
          .enableDarPackages(pkgId1, depPkgs, "test DAR description", synchronize = false)
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
            inside(topologyStateUpdateArgCaptor.value.element.mapping) {
              case VettedPackages(`participantId`, `packages`) => succeed
            }
          })
      }
    }

    "not vet the packages" when {
      "when all of them are already vetted" in withTestSetup { env =>
        import env.*
        arrange(
          notVetted = Seq(packages.toSet -> Set.empty),
          existingMappings = Seq(CheckOnlyPackages(participantId, packages) -> false),
        )

        packageOps
          .enableDarPackages(pkgId1, depPkgs, "test DAR description", synchronize = false)
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
            verify(topologyManager)
              .packagesNotVetted(participantId, packages.toSet)
            verify(topologyManager).mappingExists(
              CheckOnlyPackages(participantId, packages)
            )
            verifyNoMoreInteractions(topologyManager)
            succeed
          })
      }
    }

    "remove the existing CheckOnlyPackages mapping" in withTestSetup { env =>
      import env.*
      val mapping = CheckOnlyPackages(participantId, packages)
      arrange(
        notVetted = Seq(packages.toSet -> Set.empty),
        existingMappings = Seq(mapping -> true),
        genTransaction =
          Seq((TopologyChangeOp.Remove, mapping, pvForCheckOnlyTxs) -> revocationTxMock),
      )

      when(
        topologyManager.authorize(
          eqTo(revocationTxMock),
          eqTo(None),
          eqTo(pvForCheckOnlyTxs),
          eqTo(true),
          eqTo(false),
        )(anyTraceContext)
      ).thenReturn(EitherT.rightT(mock[SignedTopologyTransaction[Nothing]]))

      packageOps
        .enableDarPackages(pkgId1, depPkgs, "test DAR description", synchronize = false)
        .value
        .unwrap
        .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
          verify(topologyManager).packagesNotVetted(participantId, packages.toSet)
          verify(topologyManager).mappingExists(mapping)

          verify(topologyManager)
            .genTransaction(TopologyChangeOp.Remove, mapping, pvForCheckOnlyTxs)
          verify(topologyManager)
            .authorize(revocationTxMock, None, pvForCheckOnlyTxs, force = true)
          succeed
        })
    }
  }

  s"$sutName.disableDarPackages" should {
    "mark all DAR packages as checkOnly" when {
      "if some of them are not marked as checkOnly" in withTestSetup { env =>
        import env.*
        val topologyStateUpdateArgCaptor = ArgCaptor[TopologyStateUpdate[Add]]

        arrange(
          notCheckOnly = Seq(packages.toSet -> Set(pkgId2)),
          existingMappings = Seq(VettedPackages(participantId, packages) -> false),
        )

        when(
          topologyManager.authorize(
            topologyStateUpdateArgCaptor.capture,
            eqTo(None),
            eqTo(pvForCheckOnlyTxs),
            eqTo(false),
            eqTo(false),
          )(anyTraceContext)
        )
          .thenReturn(EitherT.rightT(mock[SignedTopologyTransaction[Nothing]]))

        packageOps
          .disableDarPackages(pkgId1, depPkgs, "test DAR description", synchronize = false)
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
            inside(topologyStateUpdateArgCaptor.value.element.mapping) {
              case CheckOnlyPackages(`participantId`, `packages`) => succeed
            }
          })
      }
    }

    "not mark the packages as check-only" when {
      "when all of them are already check-only" in withTestSetup { env =>
        import env.*
        arrange(
          notVetted = Seq(packages.toSet -> Set.empty),
          existingMappings = Seq(CheckOnlyPackages(participantId, packages) -> false),
        )

        packageOps
          .enableDarPackages(pkgId1, depPkgs, "test DAR description", synchronize = false)
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
            verify(topologyManager)
              .packagesNotVetted(participantId, packages.toSet)
            verify(topologyManager).mappingExists(
              CheckOnlyPackages(participantId, packages)
            )
            verifyNoMoreInteractions(topologyManager)
            succeed
          })
      }
    }

    "remove the existing vetting mapping" in withTestSetup { env =>
      import env.*
      val mapping = VettedPackages(participantId, packages)
      arrange(
        notCheckOnly = Seq(packages.toSet -> Set.empty),
        existingMappings = Seq(mapping -> true),
        genTransaction =
          Seq((TopologyChangeOp.Remove, mapping, testedProtocolVersion) -> revocationTxMock),
      )

      when(
        topologyManager.authorize(
          eqTo(revocationTxMock),
          eqTo(None),
          eqTo(testedProtocolVersion),
          eqTo(true),
          eqTo(false),
        )(anyTraceContext)
      ).thenReturn(EitherT.rightT(mock[SignedTopologyTransaction[Nothing]]))

      packageOps
        .disableDarPackages(pkgId1, depPkgs, "test DAR description", synchronize = false)
        .value
        .unwrap
        .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
          verify(topologyManager).packagesNotMarkedAsCheckOnly(participantId, packages.toSet)
          verify(topologyManager).mappingExists(mapping)
          verify(topologyManager)
            .genTransaction(TopologyChangeOp.Remove, mapping, testedProtocolVersion)
          verify(topologyManager)
            .authorize(revocationTxMock, None, testedProtocolVersion, force = true)
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
      futureSupervisor,
      timeouts,
      loggerFactory,
    )

    def arrange(
        notVetted: Seq[(Set[LfPackageId], Set[LfPackageId])] = Seq.empty,
        notCheckOnly: Seq[(Set[LfPackageId], Set[LfPackageId])] = Seq.empty,
        existingMappings: Seq[(TopologyMapping, Boolean)] = Seq.empty,
        genTransaction: Seq[
          (
              (TopologyChangeOp, TopologyPackagesStateUpdateMapping, ProtocolVersion),
              TopologyTransaction[TopologyChangeOp],
          )
        ] = Seq.empty,
    ): Unit = {
      notVetted.foreach { case (expectedArg, ret) =>
        when(
          topologyManager.packagesNotVetted(participantId, expectedArg)
        ).thenReturn(Future.successful(ret))
      }

      notCheckOnly.foreach { case (expectedArg, ret) =>
        when(
          topologyManager.packagesNotMarkedAsCheckOnly(participantId, expectedArg)
        ).thenReturn(Future.successful(ret))
      }

      existingMappings.foreach { case (mapping, exists) =>
        when(topologyManager.mappingExists(mapping)).thenReturn(Future.successful(exists))
      }

      genTransaction.foreach { case ((op, mapping, pv), tx) =>
        when(topologyManager.genTransaction(op, mapping, pv)).thenReturn(EitherT.rightT(tx))
      }
    }

    val revocationTxMock = mock[TopologyTransaction[Remove]]

    val pvForCheckOnlyTxs = Ordering[ProtocolVersion].max(
      testedProtocolVersion,
      CheckOnlyPackages.minimumSupportedProtocolVersion,
    )
  }
}
