// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import com.daml.lf.data.Ref
import com.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.canton.crypto.{Fingerprint, Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageMissingDependencies
import com.digitalasset.canton.participant.store.{
  ActiveContractStore,
  ContractStore,
  SyncDomainPersistentState,
}
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError.MainDarPackageReferencedExternally
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

class PackageOpsTest extends PackageOpsTestBase {
  protected type T = TestSetup
  protected def buildSetup: T = new TestSetup()
  protected def sutName: String = classOf[PackageOpsImpl].getSimpleName

  s"$sutName.enableDarPackages" should {
    "mark all DAR packages as vetted" when {
      "no mapping exists" in withTestSetup { env =>
        import env.*

        arrange(
          existingMappings = Seq(
            CheckOnlyPackages(participantId, packages) -> false,
            VettedPackages(participantId, packages) -> false,
          )
        )

        packageOps
          .enableDarPackages(pkgId1, depPkgs, "test DAR description", synchronize = false)
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
            inside(topologyAuthorizeTxCaptor.value.element.mapping) {
              case VettedPackages(`participantId`, `packages`) => succeed
            }
          })
      }

      "a mapping exists with a superset of packages" in withTestSetup { env =>
        import env.*

        arrange(
          existingMappings = Seq(
            CheckOnlyPackages(participantId, packages) -> false,
            VettedPackages(participantId, packages) -> false,
            VettedPackages(
              participantId,
              packages :+ Ref.PackageId.assertFromString("newPkgId"),
            ) -> true,
          )
        )

        packageOps
          .enableDarPackages(pkgId1, depPkgs, "test DAR description", synchronize = false)
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
            inside(topologyAuthorizeTxCaptor.value.element.mapping) {
              case VettedPackages(`participantId`, `packages`) => succeed
            }
          })
      }
    }

    "not vet the packages" when {
      "a matching vetting mapping already exists" in withTestSetup { env =>
        import env.*
        arrange(
          existingMappings = Seq(
            CheckOnlyPackages(participantId, packages) -> false,
            VettedPackages(participantId, packages) -> true,
          )
        )

        packageOps
          .enableDarPackages(pkgId1, depPkgs, "test DAR description", synchronize = false)
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
            verify(topologyManager).mappingExists(VettedPackages(participantId, packages))
            verify(topologyManager).mappingExists(CheckOnlyPackages(participantId, packages))
            verifyNoMoreInteractions(topologyManager)
            succeed
          })
      }
    }

    "remove the existing CheckOnlyPackages mapping" in withTestSetup { env =>
      import env.*
      arrange(
        existingMappings = Seq(checkOnlyPackagesMapping -> true, vettedPackagesMapping -> false),
        genTransaction = Seq(
          (TopologyChangeOp.Remove, checkOnlyPackagesMapping, pvForCheckOnlyTxs) -> revocationTxMock
        ),
      )

      packageOps
        .enableDarPackages(pkgId1, depPkgs, "test DAR description", synchronize = false)
        .value
        .unwrap
        .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
          verify(topologyManager).mappingExists(checkOnlyPackagesMapping)
          verify(topologyManager).mappingExists(vettedPackagesMapping)

          verify(topologyManager)
            .genTransaction(TopologyChangeOp.Remove, checkOnlyPackagesMapping, pvForCheckOnlyTxs)
          verify(topologyManager)
            .authorize(revocationTxMock, None, pvForCheckOnlyTxs, force = true)
          succeed
        })
    }

    "wait for topology transactions observed on synchronize enabled" in withTestSetup { env =>
      import env.*
      arrange(
        existingMappings = Seq(checkOnlyPackagesMapping -> true, vettedPackagesMapping -> false),
        genTransaction = Seq(
          (TopologyChangeOp.Remove, checkOnlyPackagesMapping, pvForCheckOnlyTxs) -> revocationTxMock
        ),
      )

      packageOps
        .enableDarPackages(pkgId1, depPkgs, "test DAR description", synchronize = true)
        .value
        .unwrap
        .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
          verify(topologyManager).mappingExists(checkOnlyPackagesMapping)
          verify(topologyManager).mappingExists(vettedPackagesMapping)

          verify(topologyManager)
            .genTransaction(TopologyChangeOp.Remove, checkOnlyPackagesMapping, pvForCheckOnlyTxs)

          verify(topologyManager)
            .authorize(revocationTxMock, None, pvForCheckOnlyTxs, force = true)

          verify(topologyManager)
            .authorize(
              any[TopologyStateUpdate[Add]],
              eqTo(None),
              eqTo(testedProtocolVersion),
              eqTo(false),
              eqTo(false),
            )(
              anyTraceContext
            )

          verify(topologyManager)
            .waitForPackagesBeingVetted(packages.toSet, participantId)
          verifyNoMoreInteractions(topologyManager)

          inside(topologyAuthorizeTxCaptor.values) {
            // Ensure order of the authorizations
            case addTx :: `revocationTxMock` :: Nil
                if addTx.element.mapping == vettedPackagesMapping =>
              succeed
          }
        })
    }
  }

  s"$sutName.disableDarPackages" should {
    "mark all DAR packages as checkOnly" when {
      "some of them are not marked as checkOnly" in withTestSetup { env =>
        import env.*
        arrange(
          existingMappings = Seq(vettedPackagesMapping -> false, checkOnlyPackagesMapping -> false)
        )

        packageOps
          .disableDarPackages(pkgId1, depPkgs, "test DAR description", synchronize = false)
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
            verify(topologyManager)
              .isPackageContainedInMultipleVettedTransactions(participantId, pkgId1)
            verify(topologyManager).authorize(
              any[TopologyStateUpdate[Add]],
              eqTo(None),
              eqTo(pvForCheckOnlyTxs),
              eqTo(false),
              eqTo(false),
            )(anyTraceContext)
            verify(topologyManager).mappingExists(vettedPackagesMapping)
            verify(topologyManager).mappingExists(checkOnlyPackagesMapping)

            inside(topologyAuthorizeTxCaptor.value.element.mapping) {
              case `checkOnlyPackagesMapping` => succeed
            }
            verifyNoMoreInteractions(topologyManager)
            succeed
          })
      }
    }

    "not mark the packages as check-only" when {
      "all of them are already check-only" in withTestSetup { env =>
        import env.*
        arrange(
          existingMappings = Seq(checkOnlyPackagesMapping -> true, vettedPackagesMapping -> false)
        )

        packageOps
          .disableDarPackages(pkgId1, depPkgs, "test DAR description", synchronize = false)
          .value
          .unwrap
          .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
            verify(topologyManager).mappingExists(vettedPackagesMapping)
            verify(topologyManager).mappingExists(checkOnlyPackagesMapping)

            verify(topologyManager)
              .isPackageContainedInMultipleVettedTransactions(participantId, pkgId1)
            verifyNoMoreInteractions(topologyManager)
            succeed
          })
      }
    }

    "remove the existing vetting mapping" in withTestSetup { env =>
      import env.*
      arrange(
        existingMappings = Seq(vettedPackagesMapping -> true, checkOnlyPackagesMapping -> false),
        genTransaction = Seq(
          (
            TopologyChangeOp.Remove,
            vettedPackagesMapping,
            testedProtocolVersion,
          ) -> revocationTxMock
        ),
      )

      packageOps
        .disableDarPackages(pkgId1, depPkgs, "test DAR description", synchronize = false)
        .value
        .unwrap
        .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
          verify(topologyManager).mappingExists(vettedPackagesMapping)
          verify(topologyManager).mappingExists(checkOnlyPackagesMapping)
          verify(topologyManager)
            .genTransaction(TopologyChangeOp.Remove, vettedPackagesMapping, testedProtocolVersion)
          verify(topologyManager)
            .authorize(revocationTxMock, None, testedProtocolVersion, force = true)
          succeed
        })
    }

    "disallow disabling if the DAR's main package is vetted more than once" in withTestSetup {
      env =>
        import env.*
        arrange(mainPackageVettedMultipleTimes = true)

        packageOps
          .disableDarPackages(pkgId1, depPkgs, "test DAR description", synchronize = false)
          .value
          .unwrap
          .map(inside(_) {
            case UnlessShutdown.Outcome(
                  Left(
                    MainDarPackageReferencedExternally
                      .Reject("DAR disabling", `pkgId1`, "test DAR description")
                  )
                ) =>
              verify(topologyManager)
                .isPackageContainedInMultipleVettedTransactions(participantId, pkgId1)
              verifyNoMoreInteractions(topologyManager)
              succeed
          })
    }

    "wait for topology transactions observed on synchronize enabled" in withTestSetup { env =>
      import env.*
      arrange(
        existingMappings = Seq(vettedPackagesMapping -> true, checkOnlyPackagesMapping -> false),
        genTransaction = Seq(
          (
            TopologyChangeOp.Remove,
            vettedPackagesMapping,
            testedProtocolVersion,
          ) -> revocationTxMock
        ),
      )

      packageOps
        .disableDarPackages(pkgId1, depPkgs, "test DAR description", synchronize = true)
        .value
        .unwrap
        .map(inside(_) { case UnlessShutdown.Outcome(Right(_)) =>
          verify(topologyManager)
            .isPackageContainedInMultipleVettedTransactions(participantId, pkgId1)
          verify(topologyManager).mappingExists(vettedPackagesMapping)
          verify(topologyManager).mappingExists(checkOnlyPackagesMapping)
          verify(topologyManager)
            .genTransaction(TopologyChangeOp.Remove, vettedPackagesMapping, testedProtocolVersion)
          verify(topologyManager)
            .authorize(revocationTxMock, None, testedProtocolVersion, force = true)
          verify(topologyManager)
            .authorize(
              any[TopologyStateUpdate[Add]],
              eqTo(None),
              eqTo(pvForCheckOnlyTxs),
              eqTo(false),
              eqTo(false),
            )(anyTraceContext)
          verify(topologyManager).waitForPackageBeingUnvetted(pkgId1, participantId)

          inside(topologyAuthorizeTxCaptor.values) {
            // Ensure order of the authorizations
            case addTx :: `revocationTxMock` :: Nil
                if addTx.element.mapping == checkOnlyPackagesMapping =>
              succeed
          }
          if (testedProtocolVersion >= CheckOnlyPackages.minimumSupportedProtocolVersion) {
            verify(topologyManager).waitForPackagesMarkedAsCheckOnly(packages.toSet, participantId)
          }
          verifyNoMoreInteractions(topologyManager)
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

    val checkOnlyPackagesMapping = CheckOnlyPackages(participantId, packages)
    val vettedPackagesMapping = VettedPackages(participantId, packages)

    def arrange(
        existingMappings: Seq[(TopologyMapping, Boolean)] = Seq.empty,
        genTransaction: Seq[
          (
              (TopologyChangeOp, TopologyPackagesStateUpdateMapping, ProtocolVersion),
              TopologyTransaction[TopologyChangeOp],
          )
        ] = Seq.empty,
        mainPackageVettedMultipleTimes: Boolean = false,
        waitForCheckOnly: Boolean = true,
        waitForUnvetted: Boolean = true,
        waitForVetted: Boolean = true,
    ): Unit = {

      existingMappings.foreach { case (mapping, exists) =>
        when(topologyManager.mappingExists(mapping)).thenReturn(Future.successful(exists))
      }

      genTransaction.foreach { case ((op, mapping, pv), tx) =>
        when(topologyManager.genTransaction(op, mapping, pv)).thenReturn(EitherT.rightT(tx))
      }

      when(topologyManager.isPackageContainedInMultipleVettedTransactions(participantId, pkgId1))
        .thenReturn(FutureUnlessShutdown.pure(mainPackageVettedMultipleTimes))

      when(topologyManager.waitForPackagesMarkedAsCheckOnly(packages.toSet, participantId))
        .thenReturn(EitherT.rightT(waitForCheckOnly))

      when(topologyManager.waitForPackageBeingUnvetted(pkgId1, participantId))
        .thenReturn(EitherT.rightT(waitForUnvetted))

      when(topologyManager.waitForPackagesBeingVetted(packages.toSet, participantId))
        .thenReturn(EitherT.rightT(waitForVetted))
    }

    val revocationTxMock = mock[TopologyTransaction[Remove]]

    val pvForCheckOnlyTxs = Ordering[ProtocolVersion].max(
      testedProtocolVersion,
      CheckOnlyPackages.minimumSupportedProtocolVersion,
    )

    when(
      topologyManager.authorize(
        eqTo(revocationTxMock),
        any[Option[Fingerprint]],
        any[ProtocolVersion],
        anyBoolean,
        anyBoolean,
      )(anyTraceContext)
    ).thenReturn(EitherT.rightT(mock[SignedTopologyTransaction[Nothing]]))

    val topologyAuthorizeTxCaptor = ArgCaptor[TopologyTransaction[AddRemoveChangeOp]]
    when(
      topologyManager.authorize(
        topologyAuthorizeTxCaptor.capture,
        any[Option[Fingerprint]],
        any[ProtocolVersion],
        anyBoolean,
        anyBoolean,
      )(anyTraceContext)
    )
      .thenReturn(EitherT.rightT(mock[SignedTopologyTransaction[Nothing]]))
  }
}
