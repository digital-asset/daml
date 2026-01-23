// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  NoParties,
  Participant,
  Participants,
  SingleParty,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{
  Dars,
  LedgerTestSuite,
  VettingAltDar,
  VettingDepDar,
  VettingMainDar_1_0_0,
  VettingMainDar_2_0_0,
  VettingMainDar_3_0_0_Incompatible,
  VettingMainDar_Split_Lineage_2_0_0,
}
import com.daml.ledger.api.v2.admin.package_management_service.{
  UpdateVettedPackagesForceFlag,
  UpdateVettedPackagesRequest,
  UpdateVettedPackagesResponse,
  UploadDarFileRequest,
  ValidateDarFileRequest,
  VettedPackagesChange,
  VettedPackagesRef,
}
import com.daml.ledger.api.v2.package_reference.{PriorTopologySerial, VettedPackages}
import com.daml.ledger.api.v2.package_service.{
  ListVettedPackagesRequest,
  ListVettedPackagesResponse,
  TopologyStateFilter,
}
import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.test.java.vetting_alt.alt.AltT
import com.daml.ledger.test.java.vetting_dep.dep.DepT
import com.daml.ledger.test.java.vetting_main_1_0_0.main.{
  MainT as MainT_1_0_0,
  MainTSimple as MainTSimple_1_0_0,
}
import com.daml.ledger.test.java.vetting_main_2_0_0.main.MainT as MainT_2_0_0
import com.daml.ledger.test.java.vetting_main_3_0_0.main.MainT as MainT_3_0_0
import com.daml.ledger.test.java.vetting_main_split_lineage_2_0_0.main.DifferentMainT as MainT_Split_Lineage_2_0_0
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.api.{
  DontVetAnyPackages,
  PackageMetadataFilter,
  VetAllPackages,
}
import com.digitalasset.canton.participant.admin.CantonPackageServiceError
import com.digitalasset.canton.topology.TopologyManagerError
import com.digitalasset.canton.topology.TopologyManagerError.ParticipantTopologyManagerError
import com.digitalasset.canton.util.FutureInstances.parallelFuture
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.timestamp.Timestamp
import org.scalactic.source.Position
import org.scalatest.AppendedClues
import org.scalatest.Inside.inside
import org.scalatest.Inspectors.*
import org.scalatest.compatible.Assertion
import org.scalatest.matchers.should.Matchers.*

import java.util.concurrent.atomic.AtomicReference
import java.util.zip.ZipInputStream
import scala.concurrent.{ExecutionContext, Future}

class VettingIT extends LedgerTestSuite with AppendedClues {
  private val vettingDepPkgId = Ref.PackageId.assertFromString(DepT.PACKAGE_ID)
  private val vettingAltPkgId = Ref.PackageId.assertFromString(AltT.PACKAGE_ID)
  private val vettingMainPkgIdV1 = Ref.PackageId.assertFromString(MainT_1_0_0.PACKAGE_ID)
  private val vettingMainPkgIdV2 = Ref.PackageId.assertFromString(MainT_2_0_0.PACKAGE_ID)
  private val vettingMainPkgIdV2SplitLineage =
    Ref.PackageId.assertFromString(MainT_Split_Lineage_2_0_0.PACKAGE_ID)
  private val vettingMainPkgIdV3UpgradeIncompatible =
    Ref.PackageId.assertFromString(MainT_3_0_0.PACKAGE_ID)

  private val vettingDepName = "vetting-dep"
  private val vettingMainName = "vetting-main"

  private val participant1Id: AtomicReference[Option[String]] =
    new AtomicReference[Option[String]](None)
  private def participantIdOrFail = participant1Id
    .get()
    .getOrElse(throw new IllegalStateException("participantId not yet discovered"))

  private val synchronizer1Id: AtomicReference[Option[String]] =
    new AtomicReference[Option[String]](None)
  private def synchronizerIdOrFail = synchronizer1Id
    .get()
    .getOrElse(throw new IllegalStateException("synchronizerId not yet discovered"))

  private def assertListResponseHasPkgIds(
      response: ListVettedPackagesResponse,
      hasPkgIds: Seq[Ref.PackageId],
      hasNotPkgIds: Seq[Ref.PackageId],
      alternativeSynchronizer: Option[String] = None,
      alternativeParticipant: Option[String] = None,
  )(implicit pos: Position): Assertion =
    inside(response) { case ListVettedPackagesResponse(Seq(vettedPackages), _) =>
      assertVettedPackagesHasPkgIds(
        vettedPackages,
        hasPkgIds,
        hasNotPkgIds,
        alternativeSynchronizer = alternativeSynchronizer,
        alternativeParticipant = alternativeParticipant,
      )
    }

  private def assertUpdateResponseChangedPkgIds(
      response: UpdateVettedPackagesResponse,
      hasPkgIds: Seq[Ref.PackageId] = Seq(),
      hasNotPkgIds: Seq[Ref.PackageId] = Seq(),
      alternativeSynchronizer: Option[String] = None,
      alternativeParticipant: Option[String] = None,
  )(implicit pos: Position): Assertion = {
    val hadPkgIds = hasNotPkgIds
    val hadNotPkgIds = hasPkgIds
    assertVettedPackagesHasPkgIds(
      response.getPastVettedPackages,
      hasPkgIds = hadPkgIds,
      hasNotPkgIds = hadNotPkgIds,
      alternativeSynchronizer = alternativeSynchronizer,
      alternativeParticipant = alternativeParticipant,
    )
    assertVettedPackagesHasPkgIds(
      response.getNewVettedPackages,
      hasPkgIds = hasPkgIds,
      hasNotPkgIds = hasNotPkgIds,
      alternativeSynchronizer = alternativeSynchronizer,
      alternativeParticipant = alternativeParticipant,
    )
  }

  private def assertVettedPackagesHasPkgIds(
      vettedPackages: VettedPackages,
      hasPkgIds: Seq[Ref.PackageId],
      hasNotPkgIds: Seq[Ref.PackageId],
      alternativeSynchronizer: Option[String] = None,
      alternativeParticipant: Option[String] = None,
  )(implicit pos: Position): Assertion = {
    val allPkgIds = vettedPackages.packages.map(_.packageId)
    forAll(hasPkgIds) { pkgId =>
      allPkgIds should contain(pkgId)
    }
    forAll(hasNotPkgIds) { pkgId =>
      allPkgIds should not contain pkgId
    }
    vettedPackages.synchronizerId should be(alternativeSynchronizer.getOrElse(synchronizerIdOrFail))
    vettedPackages.participantId should be(alternativeParticipant.getOrElse(participantIdOrFail))
  }

  private def assertListResponseHasPkgIdWithBounds(
      response: ListVettedPackagesResponse,
      targetPkgId: Ref.PackageId,
      expectedLowerBound: Option[Timestamp],
      expectedUpperBound: Option[Timestamp],
  )(implicit pos: Position): Assertion =
    inside(response) { case ListVettedPackagesResponse(Seq(vettedPackages), _) =>
      val matching = vettedPackages.packages.find(_.packageId == targetPkgId)
      inside(matching) { case Some(vetted) =>
        vetted.validFromInclusive shouldBe expectedLowerBound
        vetted.validUntilExclusive shouldBe expectedUpperBound
      }
    }

  private def assertResponseHasAllVersions(
      response: ListVettedPackagesResponse,
      alternativeSynchronizer: Option[String] = None,
      alternativeParticipant: Option[String] = None,
  )(implicit pos: Position): Assertion =
    assertListResponseHasPkgIds(
      response = response,
      hasPkgIds = Seq(vettingDepPkgId, vettingMainPkgIdV2, vettingMainPkgIdV1),
      hasNotPkgIds = Seq(vettingAltPkgId),
      alternativeSynchronizer = alternativeSynchronizer,
      alternativeParticipant = alternativeParticipant,
    )

  private def assertResponseHasV2(
      response: ListVettedPackagesResponse,
      alternativeSynchronizer: Option[String] = None,
      alternativeParticipant: Option[String] = None,
  )(implicit pos: Position): Assertion =
    assertListResponseHasPkgIds(
      response = response,
      hasPkgIds = Seq(vettingDepPkgId, vettingMainPkgIdV2),
      hasNotPkgIds = Seq(vettingAltPkgId, vettingMainPkgIdV1),
      alternativeSynchronizer = alternativeSynchronizer,
      alternativeParticipant = alternativeParticipant,
    )

  private def assertResponseHasDep(
      response: ListVettedPackagesResponse,
      alternativeSynchronizer: Option[String] = None,
      alternativeParticipant: Option[String] = None,
  )(implicit pos: Position): Assertion =
    assertListResponseHasPkgIds(
      response = response,
      hasPkgIds = Seq(vettingDepPkgId),
      hasNotPkgIds = Seq(vettingMainPkgIdV2, vettingAltPkgId),
      alternativeSynchronizer = alternativeSynchronizer,
      alternativeParticipant = alternativeParticipant,
    )

  private def assertResponseHasNothing(
      response: ListVettedPackagesResponse,
      alternativeSynchronizer: Option[String] = None,
      alternativeParticipant: Option[String] = None,
  )(implicit pos: Position): Assertion =
    assertListResponseHasPkgIds(
      response = response,
      hasPkgIds = Seq(),
      hasNotPkgIds = Seq(vettingDepPkgId, vettingMainPkgIdV1, vettingMainPkgIdV2, vettingAltPkgId),
      alternativeSynchronizer = alternativeSynchronizer,
      alternativeParticipant = alternativeParticipant,
    )

  private def listAllRequest: ListVettedPackagesRequest =
    listPackagesRequest(Seq(), Seq())

  private def listNamesRequest(prefixes: Seq[String]): ListVettedPackagesRequest =
    listPackagesRequest(Seq(), prefixes)

  private def listPkgIdsRequest(ids: Seq[Ref.PackageId]): ListVettedPackagesRequest =
    listPackagesRequest(ids, Seq())

  private def listPackagesRequest(
      ids: Seq[Ref.PackageId],
      namePrefixes: Seq[String],
  ): ListVettedPackagesRequest =
    ListVettedPackagesRequest(
      Some(PackageMetadataFilter(ids, namePrefixes).toProtoLAPI),
      Some(
        TopologyStateFilter(
          participantIds = Seq(participantIdOrFail),
          synchronizerIds = Seq(synchronizerIdOrFail),
        )
      ),
      "",
      0,
    )

  private def refsToVetOp(
      refs: Seq[VettedPackagesRef],
      newValidFromInclusive: Option[Timestamp] = None,
      newValidUntilExclusive: Option[Timestamp] = None,
  ) =
    VettedPackagesChange.Operation.Vet(
      VettedPackagesChange.Vet(
        refs,
        newValidFromInclusive,
        newValidUntilExclusive,
      )
    )

  private def refsToUnvetOp(refs: Seq[VettedPackagesRef]) =
    VettedPackagesChange.Operation.Unvet(
      VettedPackagesChange.Unvet(
        refs
      )
    )

  private def changeOpsRequest(
      operations: Seq[VettedPackagesChange.Operation],
      dryRun: Boolean = false,
      alternativeSynchronizer: Option[String] = None,
      expectedTopologySerial: Option[PriorTopologySerial] = None,
      allowVetIncompatibleUpgrades: Boolean = false,
      allowUnvettedDependencies: Boolean = false,
  ): UpdateVettedPackagesRequest =
    UpdateVettedPackagesRequest(
      operations.map(VettedPackagesChange(_)),
      dryRun,
      alternativeSynchronizer.getOrElse(synchronizerIdOrFail),
      expectedTopologySerial,
      Seq(
        UpdateVettedPackagesForceFlag.UPDATE_VETTED_PACKAGES_FORCE_FLAG_ALLOW_VET_INCOMPATIBLE_UPGRADES
      ).filter(_ => allowVetIncompatibleUpgrades) ++ Seq(
        UpdateVettedPackagesForceFlag.UPDATE_VETTED_PACKAGES_FORCE_FLAG_ALLOW_UNVETTED_DEPENDENCIES
      ).filter(_ => allowUnvettedDependencies),
    )

  private def changeOpRequest(
      operation: VettedPackagesChange.Operation,
      dryRun: Boolean = false,
  ): UpdateVettedPackagesRequest =
    changeOpsRequest(Seq(operation), dryRun)

  private def vetPkgIdsRequest(
      pkgIds: Seq[String],
      dryRun: Boolean = false,
      alternativeSynchronizer: Option[String] = None,
      expectedTopologySerial: Option[PriorTopologySerial] = None,
      allowVetIncompatibleUpgrades: Boolean = false,
      allowUnvettedDependencies: Boolean = false,
  ): UpdateVettedPackagesRequest =
    changeOpsRequest(
      operations =
        Seq(refsToVetOp(pkgIds.map((pkgId: String) => VettedPackagesRef(pkgId, "", "")))),
      dryRun = dryRun,
      alternativeSynchronizer = alternativeSynchronizer,
      expectedTopologySerial = expectedTopologySerial,
      allowVetIncompatibleUpgrades = allowVetIncompatibleUpgrades,
      allowUnvettedDependencies = allowUnvettedDependencies,
    )

  private def vetPkgsMatchingRef(
      participant: ParticipantTestContext,
      ref: VettedPackagesRef,
      newValidFromInclusive: Option[Timestamp] = None,
      newValidUntilExclusive: Option[Timestamp] = None,
  ): Future[UpdateVettedPackagesResponse] =
    participant
      .updateVettedPackages(
        changeOpRequest(
          refsToVetOp(
            Seq(ref),
            newValidFromInclusive,
            newValidUntilExclusive,
          )
        )
      )

  private def allPackageIds(darName: String): Seq[String] =
    DarDecoder
      .readArchive(
        darName,
        new ZipInputStream(getClass.getClassLoader.getResourceAsStream(darName)),
      )
      .toOption
      .value
      .all
      .map(_._1)

  private def mainPackageId(darName: String): String =
    DarDecoder
      .readArchive(
        darName,
        new ZipInputStream(getClass.getClassLoader.getResourceAsStream(darName)),
      )
      .toOption
      .value
      .main
      ._1

  private def vetAllInDar(
      participant: ParticipantTestContext,
      darName: String,
      alternativeSynchronizer: Option[String] = None,
      expectedTopologySerial: Option[PriorTopologySerial] = None,
  ): Future[UpdateVettedPackagesResponse] =
    participant
      .updateVettedPackages(
        vetPkgIdsRequest(
          allPackageIds(darName).map(_.toString),
          alternativeSynchronizer = alternativeSynchronizer,
          expectedTopologySerial = expectedTopologySerial,
        )
      )

  private def opDARMains(
      op: Seq[VettedPackagesRef] => VettedPackagesChange.Operation,
      participant: ParticipantTestContext,
      darNames: Seq[String],
      dryRun: Boolean,
      alternativeSynchronizer: Option[String],
      expectedTopologySerial: Option[PriorTopologySerial],
  ): Future[UpdateVettedPackagesResponse] = {
    val mainPackageIds = darNames.map(mainPackageId(_))

    participant
      .updateVettedPackages(
        changeOpsRequest(
          Seq(
            op(
              mainPackageIds.map((pkgId: String) => VettedPackagesRef(pkgId, "", ""))
            )
          ),
          dryRun = dryRun,
          alternativeSynchronizer = alternativeSynchronizer,
          expectedTopologySerial = expectedTopologySerial,
        )
      )
  }

  private def unvetDARMains(
      ledger: ParticipantTestContext,
      darNames: Seq[String],
      alternativeSynchronizer: Option[String] = None,
  ): Future[UpdateVettedPackagesResponse] =
    opDARMains(
      refsToUnvetOp,
      ledger,
      darNames,
      dryRun = false,
      alternativeSynchronizer = alternativeSynchronizer,
      expectedTopologySerial = None,
    )

  private def vetDARMains(
      ledger: ParticipantTestContext,
      darNames: Seq[String],
      dryRun: Boolean = false,
      alternativeSynchronizer: Option[String] = None,
      expectedTopologySerial: Option[PriorTopologySerial] = None,
  ): Future[UpdateVettedPackagesResponse] =
    opDARMains(
      refsToVetOp(_),
      ledger,
      darNames,
      dryRun,
      alternativeSynchronizer,
      expectedTopologySerial,
    )

  private def unvetAllDARMains(
      participant: ParticipantTestContext
  )(implicit ec: ExecutionContext): Future[Unit] =
    for {
      syncIds <- participant.connectedSynchronizers()
      _ <- MonadUtil.parTraverseWithLimit(PositiveInt.four)(syncIds)(syncId =>
        unvetDARMains(
          participant,
          Seq(
            VettingDepDar.path,
            VettingMainDar_1_0_0.path,
            VettingMainDar_2_0_0.path,
            VettingAltDar.path,
            VettingMainDar_Split_Lineage_2_0_0.path,
            VettingMainDar_3_0_0_Incompatible.path,
          ),
          alternativeSynchronizer = Some(syncId),
        )
      )
    } yield ()

  private def setNodeIds(
      participant: ParticipantTestContext
  )(implicit ec: ExecutionContext): Future[Unit] =
    for {
      _ <- participant.getParticipantId().map { participantId =>
        participant1Id.set(Some(participantId))
      }
      _ <- participant.connectedSynchronizers().map { connected =>
        connected.find(_.startsWith("synchronizer1")).foreach { syncId =>
          synchronizer1Id.set(Some(syncId))
        }
      }
    } yield ()

  private def getSynchronizerId(
      participant: ParticipantTestContext,
      syncIndex: Int,
  )(implicit ec: ExecutionContext): Future[String] =
    for {
      participantId <- participant.getParticipantId()
      result <- participant
        .connectedSynchronizers()
        .map(
          _.find(_.startsWith("synchronizer" ++ syncIndex.toString)).getOrElse(
            sys.error(
              s"Connection between participant $participantId and synchronizer 'synchronizer$syncIndex' via ${participant.userId} / ${participant.endpointId} does not exist."
            )
          )
        )
    } yield result

  private def uploadDarFileDontVetRequest(path: String) =
    UploadDarFileRequest(
      darFile = Dars.read(path),
      "",
      DontVetAnyPackages.toProto,
      "",
    )

  test(
    "PVListVettedPackagesBasic",
    "Listing all, listing by name, listing by pkgId, listing by pkgId and name",
    allocate(NoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, _)) =>
    for {
      _ <- setNodeIds(participant)
      _ <- participant.uploadDarFileAndVetOnConnectedSynchronizers(Dars.read(VettingDepDar.path))
      _ <- participant.uploadDarFileAndVetOnConnectedSynchronizers(
        Dars.read(VettingMainDar_1_0_0.path)
      )
      _ <- participant.uploadDarFileAndVetOnConnectedSynchronizers(
        Dars.read(VettingMainDar_2_0_0.path)
      )
      _ <- participant.uploadDarFile(
        uploadDarFileDontVetRequest(VettingAltDar.path)
      )

      allResponse <- participant.listVettedPackages(listAllRequest)
      depNameResponse <- participant.listVettedPackages(listNamesRequest(Seq(vettingDepName)))
      bothNameResponse <- participant.listVettedPackages(
        listNamesRequest(Seq(vettingDepName, vettingMainName))
      )
      depPkgIdResponse <- participant.listVettedPackages(listPkgIdsRequest(Seq(vettingDepPkgId)))
      bothPkgIdResponse <- participant.listVettedPackages(
        listPkgIdsRequest(Seq(vettingDepPkgId, vettingMainPkgIdV2))
      )
      depPkgIdAndNameResponse <- participant.listVettedPackages(
        listPackagesRequest(Seq(vettingDepPkgId), Seq(vettingDepName))
      )
      bothPkgIdAndNamesResponse <- participant.listVettedPackages(
        listPackagesRequest(
          Seq(vettingDepPkgId, vettingMainPkgIdV2),
          Seq(vettingDepName, vettingMainName),
        )
      )
      commonPrefixResponse <- participant.listVettedPackages(listNamesRequest(Seq("vetting-")))
      disjointPkgIdAndNameResponse <- participant.listVettedPackages(
        listPackagesRequest(Seq(vettingDepPkgId), Seq(vettingMainName))
      )

      _ <- unvetAllDARMains(participant)
    } yield {
      assertResponseHasAllVersions(allResponse)
      assertResponseHasDep(depNameResponse)
      assertResponseHasAllVersions(bothNameResponse)
      assertResponseHasDep(depPkgIdResponse)
      assertResponseHasV2(bothPkgIdResponse)
      assertResponseHasDep(depPkgIdAndNameResponse)
      assertResponseHasAllVersions(bothPkgIdAndNamesResponse)
      assertResponseHasAllVersions(commonPrefixResponse)
      assertResponseHasAllVersions(disjointPkgIdAndNameResponse)
    }
  })

  test(
    "PVUploadDarFileBasic",
    "Uploading DAR files vets all packages by default, including dependencies",
    allocate(NoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, _)) =>
    for {
      _ <- setNodeIds(participant)
      _ <- participant.uploadDarFileAndVetOnConnectedSynchronizers(
        Dars.read(VettingMainDar_2_0_0.path)
      ) // Should vet vetting-dep dependency
      _ <- participant.uploadDarFile(
        uploadDarFileDontVetRequest(VettingAltDar.path)
      )

      allResponse <- participant.listVettedPackages(listAllRequest)
      _ <- unvetAllDARMains(participant)
    } yield {
      assertResponseHasV2(allResponse)
    }
  })

  def updateTest(
      title: String,
      description: String,
      act: ExecutionContext => ParticipantTestContext => Future[Assertion],
  ) =
    test(
      title,
      description,
      allocate(NoParties),
      runConcurrently = false,
    )(implicit ec => { case Participants(Participant(participant, _)) =>
      for {
        _ <- setNodeIds(participant)
        _ <- participant.uploadDarFile(
          uploadDarFileDontVetRequest(VettingDepDar.path)
        )
        _ <- participant.uploadDarFile(
          uploadDarFileDontVetRequest(VettingMainDar_1_0_0.path)
        )
        _ <- participant.uploadDarFile(
          uploadDarFileDontVetRequest(VettingMainDar_2_0_0.path)
        )
        _ <- participant.uploadDarFile(
          uploadDarFileDontVetRequest(VettingAltDar.path)
        )
        _ <- unvetAllDARMains(participant)
        _ <- act(ec)(participant)
        _ <- unvetAllDARMains(participant)
      } yield {
        ()
      }
    })

  updateTest(
    "PVUpdateVetDepThenV2Succeeds",
    "Successfully vet everything in vetting-dep and then just the main package from vetting-main",
    implicit ec =>
      (participant: ParticipantTestContext) =>
        for {
          vetDep <- vetAllInDar(participant, VettingDepDar.path)
          vetMain <- vetDARMains(participant, Seq(VettingMainDar_2_0_0.path))

          allResponse <- participant.listVettedPackages(listAllRequest)
        } yield {
          assertResponseHasV2(allResponse)
          assertUpdateResponseChangedPkgIds(vetDep, hasPkgIds = Seq(vettingDepPkgId))
          assertUpdateResponseChangedPkgIds(vetMain, hasPkgIds = Seq(vettingMainPkgIdV2))
        },
  )

  updateTest(
    "PVUpdateUnvetV2Succeeds",
    "Successfully vet everything in vetting-main then just unvet the main package from vetting-main",
    implicit ec =>
      (participant: ParticipantTestContext) =>
        for {
          _ <- vetAllInDar(participant, VettingMainDar_2_0_0.path)
          unvetMain <- unvetDARMains(participant, Seq(VettingMainDar_2_0_0.path))

          unvetTestAllResponse <- participant.listVettedPackages(listAllRequest)
        } yield {
          assertResponseHasDep(unvetTestAllResponse)
          assertUpdateResponseChangedPkgIds(unvetMain, hasNotPkgIds = Seq(vettingMainPkgIdV2))
        },
  )

  updateTest(
    "PVUpdateVetTwoPackagesAtATimeSucceeds",
    "Successfully vet both vetting-dep and vetting-main in one update",
    implicit ec =>
      (participant: ParticipantTestContext) =>
        for {
          vetBoth <- participant.updateVettedPackages(
            vetPkgIdsRequest(Seq(vettingMainPkgIdV2, vettingDepPkgId))
          )

          vetMainAndDepResponse <- participant.listVettedPackages(listAllRequest)
        } yield {
          assertResponseHasV2(vetMainAndDepResponse)
          assertUpdateResponseChangedPkgIds(
            vetBoth,
            hasPkgIds = Seq(vettingDepPkgId, vettingMainPkgIdV2),
          )
        },
  )

  updateTest(
    "PVUpdateVetMultipleByNameFails",
    "Successfully vet multiple packages by name",
    implicit ec =>
      (participant: ParticipantTestContext) =>
        for {
          _ <- vetAllInDar(participant, VettingDepDar.path)
          _ <- vetPkgsMatchingRef(participant, VettedPackagesRef("", vettingMainName, ""))
            .mustFailWith(
              "Vetting multiple packages with a single reference should give AMBIGUOUS_VETTING_REFERENCE",
              CantonPackageServiceError.Vetting.VettingReferenceMoreThanOne,
            )
        } yield succeed,
  )

  updateTest(
    "PVUpdateVetNonexistentNameFails",
    "Fail to vet a package by a nonexistent name",
    implicit ec =>
      (participant: ParticipantTestContext) =>
        for {
          _ <- vetAllInDar(participant, VettingDepDar.path)
          _ <- vetPkgsMatchingRef(participant, VettedPackagesRef("", "nonexistent-name", ""))
            .mustFailWith(
              "Vetting a nonexistent name in a reference should give UNRESOLVED_VETTING_REFERENCE",
              CantonPackageServiceError.Vetting.VettingReferenceEmpty,
            )
        } yield succeed,
  )

  updateTest(
    "PVUpdateVetByNameAndVersion",
    "Successfully vet a package by name and version",
    implicit ec =>
      (participant: ParticipantTestContext) =>
        for {
          _ <- vetAllInDar(participant, VettingDepDar.path)
          vetMain <- vetPkgsMatchingRef(
            participant,
            VettedPackagesRef("", vettingMainName, ("2.0.0")),
          )
          vetOnlyV2ByNameAndVersionResponse <- participant.listVettedPackages(listAllRequest)
        } yield {
          assertResponseHasV2(vetOnlyV2ByNameAndVersionResponse)
          assertUpdateResponseChangedPkgIds(vetMain, hasPkgIds = Seq(vettingMainPkgIdV2))
        },
  )

  updateTest(
    "PVUpdateVetByNameAndNonexistentVersion",
    "Fail when trying to vet nonexistent version of a package name that exists",
    implicit ec =>
      (participant: ParticipantTestContext) =>
        for {
          _ <- vetAllInDar(participant, VettingDepDar.path)
          _ <- vetPkgsMatchingRef(
            participant,
            VettedPackagesRef("", vettingMainName, ("3.0.0")),
          ).mustFailWith(
            "Vetting an existing name with a nonexistent version should give UNRESOLVED_VETTING_REFERENCE",
            CantonPackageServiceError.Vetting.VettingReferenceEmpty,
          )
        } yield succeed,
  )

  updateTest(
    "PVUpdateVetByIdAndNameAndVersion",
    "Successfully vet a package by ID, name, version when all three match",
    implicit ec =>
      (participant: ParticipantTestContext) =>
        for {
          _ <- vetAllInDar(participant, VettingDepDar.path)
          vetMain <- vetPkgsMatchingRef(
            participant,
            VettedPackagesRef(
              vettingMainPkgIdV2,
              vettingMainName,
              "2.0.0",
            ),
          )
          vetOnlyV2ByAllResponse <- participant.listVettedPackages(listAllRequest)
        } yield {
          assertResponseHasV2(vetOnlyV2ByAllResponse)
          assertUpdateResponseChangedPkgIds(vetMain, hasPkgIds = Seq(vettingMainPkgIdV2))
        },
  )

  updateTest(
    "PVUpdateVetByIdWithWrongName",
    "Fail to vet a package by ID when paired with the wrong name",
    implicit ec =>
      (participant: ParticipantTestContext) =>
        for {
          _ <- vetAllInDar(participant, VettingDepDar.path)
          _ <- vetPkgsMatchingRef(
            participant,
            VettedPackagesRef(
              vettingMainPkgIdV2,
              "nonexistent-name",
              "2.0.0",
            ),
          ).mustFailWith(
            "Vetting an existing pkg ID with the incorrect name should give VETTING_REFERENCE_EMPTY",
            CantonPackageServiceError.Vetting.VettingReferenceEmpty,
          )
        } yield succeed,
  )

  updateTest(
    "PVUpdateVetByIdWithWrongVersion",
    "Fail to vet a package by ID when paired with the wrong version",
    implicit ec =>
      (participant: ParticipantTestContext) =>
        for {
          _ <- vetAllInDar(participant, VettingDepDar.path)
          _ <- vetPkgsMatchingRef(
            participant,
            VettedPackagesRef(
              vettingMainPkgIdV2,
              vettingMainName,
              "3.0.0",
            ),
          ).mustFailWith(
            "Vetting an existing pkg ID with the incorrect name should give VETTING_REFERENCE_EMPTY",
            CantonPackageServiceError.Vetting.VettingReferenceEmpty,
          )
        } yield succeed,
  )

  updateTest(
    "PVUpdateVetWithSerial",
    "Vet packages if the expected topology serial matches the last transaction serial",
    implicit ec =>
      (ledger: ParticipantTestContext) =>
        for {
          response0 <- ledger.listVettedPackages(listAllRequest)
          priorSerial0 = response0.vettedPackages(0).topologySerial

          response1 <- vetAllInDar(
            ledger,
            VettingDepDar.path,
            expectedTopologySerial = Some(
              PriorTopologySerial(PriorTopologySerial.Serial.Prior(priorSerial0))
            ),
          )
          _ = response1.getPastVettedPackages.topologySerial shouldBe priorSerial0
          _ = response1.getNewVettedPackages.topologySerial should not be priorSerial0
          priorSerial1 = response1.getNewVettedPackages.topologySerial

          response2 <- vetDARMains(
            ledger,
            Seq(VettingMainDar_2_0_0.path),
            expectedTopologySerial = Some(
              PriorTopologySerial(PriorTopologySerial.Serial.Prior(priorSerial1))
            ),
          )
        } yield {
          response2.getPastVettedPackages.topologySerial shouldBe priorSerial1
          response2.getNewVettedPackages.topologySerial should not be priorSerial1
        },
  )

  updateTest(
    "PVUpdateVetWithWrongSerial",
    "Fail to vet packages if the expected topology serial does not match the last transaction serial",
    implicit ec =>
      (ledger: ParticipantTestContext) =>
        for {
          response1 <- vetAllInDar(ledger, VettingDepDar.path)
          priorSerial = response1.getNewVettedPackages.topologySerial
          _ = priorSerial should not be 1

          response2 <- vetDARMains(
            ledger,
            Seq(VettingMainDar_2_0_0.path),
            expectedTopologySerial = Some(PriorTopologySerial(PriorTopologySerial.Serial.Prior(1))),
          ).mustFailWith(
            s"Should fail because of prior serial $priorSerial does not match 1",
            TopologyManagerError.SerialMismatch,
          )
        } yield succeed,
  )

  updateTest(
    "PVDryRunWithWrongSerial",
    "Fail to vet with dry-run if the expected topology serial does not match the last transaction serial",
    implicit ec =>
      (ledger: ParticipantTestContext) =>
        for {
          response1 <- vetAllInDar(ledger, VettingDepDar.path)
          priorSerial = response1.getNewVettedPackages.topologySerial
          _ = priorSerial should not be 1

          response2 <- vetDARMains(
            ledger,
            Seq(VettingMainDar_2_0_0.path),
            dryRun = true,
            expectedTopologySerial = Some(PriorTopologySerial(PriorTopologySerial.Serial.Prior(1))),
          ).mustFailWith(
            s"Should fail because of prior serial $priorSerial does not match 1",
            TopologyManagerError.SerialMismatch,
          )
        } yield succeed,
  )

  test(
    "PVListVettedPackagesNothingVetted",
    "Listing vetted packages returns nothing when uploading DARs without vetting them",
    allocate(NoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, _)) =>
    for {
      _ <- setNodeIds(participant)
      _ <- participant.uploadDarFile(
        uploadDarFileDontVetRequest(VettingDepDar.path)
      )
      _ <- participant.uploadDarFile(
        uploadDarFileDontVetRequest(VettingMainDar_2_0_0.path)
      )
      _ <- participant.uploadDarFile(
        uploadDarFileDontVetRequest(VettingAltDar.path)
      )

      allResponse <- participant.listVettedPackages(listAllRequest)

      _ <- unvetAllDARMains(participant)
    } yield {
      assertResponseHasNothing(allResponse)
    }
  })

  test(
    "PVDryRun",
    "Vetting with Dry Run returns the expected changes, but does not commit them",
    allocate(NoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, _)) =>
    for {
      _ <- setNodeIds(participant)
      _ <- participant.uploadDarFile(
        uploadDarFileDontVetRequest(VettingMainDar_2_0_0.path)
      )
      _ <- participant.uploadDarFile(
        uploadDarFileDontVetRequest(VettingAltDar.path)
      )
      dryRunUpdateResponse <- participant.updateVettedPackages(
        vetPkgIdsRequest(
          pkgIds = Seq(vettingAltPkgId),
          dryRun = true,
        )
      )

      listAllResponse <- participant.listVettedPackages(listAllRequest)
      _ <- unvetAllDARMains(participant)
    } yield {
      assertVettedPackagesHasPkgIds(
        vettedPackages = dryRunUpdateResponse.getPastVettedPackages,
        hasPkgIds = Seq(),
        hasNotPkgIds = Seq(vettingDepPkgId, vettingMainPkgIdV2, vettingAltPkgId),
      )

      assertVettedPackagesHasPkgIds(
        vettedPackages = dryRunUpdateResponse.getNewVettedPackages,
        hasPkgIds = Seq(vettingAltPkgId),
        hasNotPkgIds = Seq(vettingDepPkgId, vettingMainPkgIdV2),
      )

      assertListResponseHasPkgIds(
        response = listAllResponse,
        hasPkgIds = Seq(),
        hasNotPkgIds = Seq(vettingDepPkgId, vettingMainPkgIdV2, vettingAltPkgId),
      )
    }
  })

  test(
    "PVWritingAndOverwritingBounds",
    "Vetting bounds can be written and overwritten",
    allocate(NoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, _)) =>
    for {
      _ <- setNodeIds(participant)
      _ <- participant.uploadDarFile(
        uploadDarFileDontVetRequest(VettingMainDar_1_0_0.path)
      )
      _ <- participant.uploadDarFile(
        uploadDarFileDontVetRequest(VettingAltDar.path)
      )
      _ <- vetAllInDar(participant, VettingDepDar.path)

      _ <- vetPkgsMatchingRef(
        participant,
        VettedPackagesRef(vettingMainPkgIdV2, "", ""),
        Some(Timestamp.of(0, 0)),
        Some(Timestamp.of(1, 0)),
      )

      listAfterBounds1 <- participant.listVettedPackages(listAllRequest)

      _ <- participant.updateVettedPackages(
        changeOpRequest(
          refsToVetOp(
            Seq(
              VettedPackagesRef(vettingMainPkgIdV2, "", ""),
              VettedPackagesRef(vettingAltPkgId, "", ""),
            ),
            Some(Timestamp.of(2, 0)),
            Some(Timestamp.of(3, 0)),
          )
        )
      )

      listAfterBounds2 <- participant.listVettedPackages(listAllRequest)

      _ <- unvetAllDARMains(participant)
    } yield {
      assertListResponseHasPkgIdWithBounds(
        listAfterBounds1,
        vettingMainPkgIdV2,
        Some(Timestamp.of(0, 0)),
        Some(Timestamp.of(1, 0)),
      )

      assertListResponseHasPkgIdWithBounds(
        listAfterBounds2,
        vettingMainPkgIdV2,
        Some(Timestamp.of(2, 0)),
        Some(Timestamp.of(3, 0)),
      )

      assertListResponseHasPkgIdWithBounds(
        listAfterBounds2,
        vettingAltPkgId,
        Some(Timestamp.of(2, 0)),
        Some(Timestamp.of(3, 0)),
      )
    }
  })

  test(
    "PVValidateDarCheckUpgradeInvariants",
    "Upgrade invariants are checked, including during validate dar request",
    allocate(NoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, _)) =>
    for {
      _ <- setNodeIds(participant)
      _ <- participant.uploadDarFile(
        UploadDarFileRequest(
          Dars.read(VettingMainDar_2_0_0.path),
          "",
          VetAllPackages.toProto,
          synchronizerIdOrFail,
        )
      )

      _ <- participant
        .validateDarFile(
          ValidateDarFileRequest(
            Dars.read(VettingMainDar_Split_Lineage_2_0_0.path),
            "",
            synchronizerId = synchronizerIdOrFail,
          )
        )
        .mustFailWith(
          "Update should fail to vet package with same name and version with KNOWN_PACKAGE_VERSION error",
          ParticipantTopologyManagerError.UpgradeVersion,
        )

      _ <- participant
        .validateDarFile(
          ValidateDarFileRequest(
            Dars.read(VettingMainDar_3_0_0_Incompatible.path),
            "",
            synchronizerId = synchronizerIdOrFail,
          )
        )
        .mustFailWith(
          "Validating an upgrade-incompatible package should yield NOT_VALID_UPGRADE_PACKAGE error",
          ParticipantTopologyManagerError.Upgradeability,
        )

      _ <- unvetAllDARMains(participant)
    } yield ()
  })

  test(
    "PVCheckUnvettedPackagesExceptWithForceFlag",
    """Unvetted packages are checked, including during dry run, except when UPDATE_VETTED_PACKAGES_FORCE_FLAG_ALLOW_UNVETTED_DEPENDENCIES is set.""",
    allocate(NoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, _)) =>
    for {
      _ <- setNodeIds(participant)
      _ <- participant.uploadDarFile(
        uploadDarFileDontVetRequest(VettingDepDar.path)
      )
      _ <- participant.uploadDarFile(
        uploadDarFileDontVetRequest(VettingMainDar_2_0_0.path)
      )

      vetMainWithoutDepRequest = vetPkgIdsRequest(Seq(vettingMainPkgIdV2))
      forceFlags = Seq(
        UpdateVettedPackagesForceFlag.UPDATE_VETTED_PACKAGES_FORCE_FLAG_ALLOW_UNVETTED_DEPENDENCIES
      )

      // Dry-run vetting without dependencies (should fail)
      _ <- participant
        .updateVettedPackages(vetMainWithoutDepRequest.copy(dryRun = true))
        .mustFailWith(
          "Vetting a package without its dependencies in a dry run should give TOPOLOGY_DEPENDENCIES_NOT_VETTED",
          ParticipantTopologyManagerError.DependenciesNotVetted,
        )

      // Vet without dependencies (should fail)
      _ <- participant
        .updateVettedPackages(vetMainWithoutDepRequest)
        .mustFailWith(
          "Vetting a package without its dependencies should give TOPOLOGY_DEPENDENCIES_NOT_VETTED",
          ParticipantTopologyManagerError.DependenciesNotVetted,
        )

      // Dry-run vetting without dependencies with force flag (should succeed)
      _ <- participant.updateVettedPackages(
        vetMainWithoutDepRequest.copy(dryRun = true, updateVettedPackagesForceFlags = forceFlags)
      )

      // Vet without dependencies with force flag (should succeed)
      _ <- participant.updateVettedPackages(
        vetMainWithoutDepRequest.copy(updateVettedPackagesForceFlags = forceFlags)
      )

      _ <- unvetAllDARMains(participant)
      _ <- vetAllInDar(participant, VettingDepDar.path)

      vetMainWhileUnvettingDepOps =
        Seq(
          refsToVetOp(
            Seq(VettedPackagesRef(vettingMainPkgIdV2, "", ""))
          ),
          refsToUnvetOp(
            Seq(VettedPackagesRef(vettingDepPkgId, "", ""))
          ),
        )

      // Dry run vet a package while unvetting its dependencies (should fail)
      _ <- participant
        .updateVettedPackages(changeOpsRequest(vetMainWhileUnvettingDepOps))
        .mustFailWith(
          "Vetting a package while unvetting its dependencies should give TOPOLOGY_DEPENDENCIES_NOT_VETTED",
          ParticipantTopologyManagerError.DependenciesNotVetted,
        )

      // Vet a package while unvetting its dependencies (should fail)
      _ <- participant
        .updateVettedPackages(changeOpsRequest(vetMainWhileUnvettingDepOps))
        .mustFailWith(
          "Vetting a package while unvetting its dependencies should give TOPOLOGY_DEPENDENCIES_NOT_VETTED",
          ParticipantTopologyManagerError.DependenciesNotVetted,
        )

      // Vet a package while unvetting its dependencies, dry run, with force flag (should succeed)
      _ <- participant
        .updateVettedPackages(
          changeOpsRequest(
            vetMainWhileUnvettingDepOps,
            dryRun = true,
            allowUnvettedDependencies = true,
          )
        )

      // Vet a package while unvetting its dependencies, with force flag (should succeed)
      _ <- participant
        .updateVettedPackages(
          changeOpsRequest(vetMainWhileUnvettingDepOps, allowUnvettedDependencies = true)
        )

      _ <- unvetAllDARMains(participant)
    } yield ()
  })

  test(
    "PVCheckUpgradeInvariantsExceptWithForceFlag",
    """Upgrade invariants are checked, including dry run, except when UPDATE_VETTED_PACKAGES_FORCE_FLAG_ALLOW_VET_INCOMPATIBLE_UPGRADES is set.""",
    allocate(NoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, _)) =>
    for {
      _ <- setNodeIds(participant)
      _ <- participant.uploadDarFile(
        uploadDarFileDontVetRequest(VettingMainDar_2_0_0.path)
      )
      _ <- participant.uploadDarFile(
        uploadDarFileDontVetRequest(VettingMainDar_3_0_0_Incompatible.path)
      )
      _ <- participant.uploadDarFile(
        uploadDarFileDontVetRequest(VettingMainDar_Split_Lineage_2_0_0.path)
      )

      _ <- vetAllInDar(participant, VettingDepDar.path)

      // Vet two upgrade-incompatible packages
      upgradeIncompatibleVet = vetPkgIdsRequest(
        Seq(vettingMainPkgIdV2, vettingMainPkgIdV3UpgradeIncompatible)
      )
      forceFlags = Seq(
        UpdateVettedPackagesForceFlag.UPDATE_VETTED_PACKAGES_FORCE_FLAG_ALLOW_VET_INCOMPATIBLE_UPGRADES
      )

      // Dry run, without the force flag (should fail)
      _ <- participant
        .updateVettedPackages(upgradeIncompatibleVet.copy(dryRun = true))
        .mustFailWith(
          "Vetting an upgrade-incompatible package without the force flag should give NOT_VALID_UPGRADE_PACKAGE error",
          ParticipantTopologyManagerError.Upgradeability,
        )

      // Vet, without the force flag (should fail)
      _ <- participant
        .updateVettedPackages(upgradeIncompatibleVet)
        .mustFailWith(
          "Vetting an upgrade-incompatible package without the force flag should give NOT_VALID_UPGRADE_PACKAGE error",
          ParticipantTopologyManagerError.Upgradeability,
        )

      // Dry-run, with force flag (should succeed)
      _ <- participant.updateVettedPackages(
        upgradeIncompatibleVet.copy(updateVettedPackagesForceFlags = forceFlags, dryRun = true)
      )

      // Vet, with force flag (should succeed)
      _ <- participant.updateVettedPackages(
        upgradeIncompatibleVet.copy(updateVettedPackagesForceFlags = forceFlags)
      )

      // Vet two packages with the same version
      sameVersionVet = vetPkgIdsRequest(Seq(vettingMainPkgIdV2, vettingMainPkgIdV2SplitLineage))

      // Dry run without force flag (should fail)
      _ <- participant
        .updateVettedPackages(sameVersionVet.copy(dryRun = true))
        .mustFailWith(
          "Update should fail to vet package with same name and version with KNOWN_PACKAGE_VERSION error",
          ParticipantTopologyManagerError.UpgradeVersion,
        )

      // Vet without force flag (should fail)
      _ <- participant
        .updateVettedPackages(sameVersionVet)
        .mustFailWith(
          "Update should fail to vet package with same name and version with KNOWN_PACKAGE_VERSION error",
          ParticipantTopologyManagerError.UpgradeVersion,
        )

      // Dry-run with force flag (should succeed)
      _ <- participant.updateVettedPackages(
        sameVersionVet.copy(updateVettedPackagesForceFlags = forceFlags, dryRun = true)
      )

      // Vet with force flag (should succeed)
      _ <- participant.updateVettedPackages(
        sameVersionVet.copy(updateVettedPackagesForceFlags = forceFlags)
      )

      _ <- unvetAllDARMains(participant)
    } yield ()
  })

  private def assertVettedOnParticipantAndSynchronizer(
      participantId: String,
      syncId: String,
      pkgId: String,
      response: ListVettedPackagesResponse,
  ): Assertion =
    inside(response) { case ListVettedPackagesResponse(vettedPackagesList, _) =>
      val matching = vettedPackagesList.find(vettedPackages =>
        vettedPackages.participantId == participantId && vettedPackages.synchronizerId == syncId
      )
      inside(matching) { case Some(actualVetted) =>
        actualVetted.packages.map(_.packageId) should contain(pkgId)
      }
    }

  private def assertUnvettedOnParticipantAndSynchronizer(
      participantId: String,
      syncId: String,
      pkgId: String,
      response: ListVettedPackagesResponse,
  ): Assertion =
    inside(response) { case ListVettedPackagesResponse(vettedPackagesList, _) =>
      val matching = vettedPackagesList.find(vettedPackages =>
        vettedPackages.participantId == participantId && vettedPackages.synchronizerId == syncId
      )
      inside(matching) { case Some(actualVetted) =>
        actualVetted.packages.map(_.packageId) should not contain pkgId
      }
    }

  private def assertParticipantAndSynchronizerNotInResponse(
      participantId: String,
      syncId: String,
      response: ListVettedPackagesResponse,
  ): Assertion =
    inside(response) { case ListVettedPackagesResponse(vettedPackagesList, _) =>
      val matching = vettedPackagesList.find(vettedPackages =>
        vettedPackages.participantId == participantId && vettedPackages.synchronizerId == syncId
      )
      matching shouldBe empty
    }

  test(
    "PVListVettedPackagesMultiSynchronizer",
    "Listing packages on a per-synchronizer and per-participant basis",
    allocate(NoParties, NoParties).expectingMinimumNumberOfSynchronizers(2),
    runConcurrently = false,
  )(implicit ec => {
    case Participants(Participant(participant1, _), Participant(participant2, _)) =>
      for {
        _ <- unvetAllDARMains(participant1)
        _ <- unvetAllDARMains(participant2)

        participant1Id <- participant1.getParticipantId()
        participant2Id <- participant2.getParticipantId()
        sync1 <- getSynchronizerId(participant1, syncIndex = 1)
        sync2 <- getSynchronizerId(participant1, syncIndex = 2)
        _ <- getSynchronizerId(participant2, syncIndex = 1)
        _ <- getSynchronizerId(participant2, syncIndex = 2)

        _ <- participant1.uploadDarFile(
          UploadDarFileRequest(
            Dars.read(VettingMainDar_2_0_0.path),
            "",
            DontVetAnyPackages.toProto,
            "",
          )
        )
        _ <- participant2.uploadDarFile(
          UploadDarFileRequest(
            Dars.read(VettingMainDar_2_0_0.path),
            "",
            DontVetAnyPackages.toProto,
            "",
          )
        )
        _ <- participant2.uploadDarFile(
          UploadDarFileRequest(
            Dars.read(VettingAltDar.path),
            "",
            DontVetAnyPackages.toProto,
            "",
          )
        )

        _ <- vetAllInDar(participant1, VettingMainDar_2_0_0.path, Some(sync1))
        _ <- vetAllInDar(participant2, VettingMainDar_2_0_0.path, Some(sync1))
        _ <- vetAllInDar(participant1, VettingMainDar_2_0_0.path, Some(sync2))
        _ <- vetAllInDar(participant2, VettingAltDar.path, Some(sync2))

        // Test that a wildcard query (a list request with no fields set) lists
        // all of the packages on all participants on all synchronizers for
        // participant1
        participant1AllResponse <- participant1.listVettedPackages(
          ListVettedPackagesRequest(
            Some(PackageMetadataFilter(Seq(), Seq()).toProtoLAPI),
            Some(TopologyStateFilter(participantIds = Seq(), synchronizerIds = Seq())),
            "",
            0,
          )
        )

        _ = {
          assertVettedOnParticipantAndSynchronizer(
            participant1Id,
            sync1,
            vettingMainPkgIdV2,
            participant1AllResponse,
          )
          assertVettedOnParticipantAndSynchronizer(
            participant2Id,
            sync1,
            vettingMainPkgIdV2,
            participant1AllResponse,
          )
          assertVettedOnParticipantAndSynchronizer(
            participant1Id,
            sync2,
            vettingMainPkgIdV2,
            participant1AllResponse,
          )
          assertUnvettedOnParticipantAndSynchronizer(
            participant2Id,
            sync2,
            vettingMainPkgIdV2,
            participant1AllResponse,
          )
        }

        // Test that the response for a wildcard query on participant2 should be the
        // same as for participant1, because they're connected to the same
        // synchronizers, modulo package metadata.
        participant2AllResponse <- participant2.listVettedPackages(
          ListVettedPackagesRequest(
            Some(PackageMetadataFilter(Seq(), Seq()).toProtoLAPI),
            Some(TopologyStateFilter(participantIds = Seq(), synchronizerIds = Seq())),
            "",
            0,
          )
        )

        _ = {
          assertVettedOnParticipantAndSynchronizer(
            participant1Id,
            sync1,
            vettingMainPkgIdV2,
            participant2AllResponse,
          )
          assertVettedOnParticipantAndSynchronizer(
            participant2Id,
            sync1,
            vettingMainPkgIdV2,
            participant2AllResponse,
          )
          assertVettedOnParticipantAndSynchronizer(
            participant1Id,
            sync2,
            vettingMainPkgIdV2,
            participant2AllResponse,
          )
          assertUnvettedOnParticipantAndSynchronizer(
            participant2Id,
            sync2,
            vettingMainPkgIdV2,
            participant2AllResponse,
          )
        }

        // Test that explicitly listing both participants and both synchronizers
        // should give the same result as the wildcard query on participant1.
        participant1AllResponseExplicit <- participant1.listVettedPackages(
          ListVettedPackagesRequest(
            Some(PackageMetadataFilter(Seq(), Seq()).toProtoLAPI),
            Some(
              TopologyStateFilter(
                participantIds = Seq(participant1Id, participant2Id),
                synchronizerIds = Seq(sync1, sync2),
              )
            ),
            "",
            0,
          )
        )

        _ = {
          assertVettedOnParticipantAndSynchronizer(
            participant1Id,
            sync1,
            vettingMainPkgIdV2,
            participant1AllResponseExplicit,
          )
          assertVettedOnParticipantAndSynchronizer(
            participant2Id,
            sync1,
            vettingMainPkgIdV2,
            participant1AllResponseExplicit,
          )
          assertVettedOnParticipantAndSynchronizer(
            participant1Id,
            sync2,
            vettingMainPkgIdV2,
            participant1AllResponseExplicit,
          )
          assertUnvettedOnParticipantAndSynchronizer(
            participant2Id,
            sync2,
            vettingMainPkgIdV2,
            participant1AllResponseExplicit,
          )
        }

        // Test that restricting the query to one participant on both
        // synchronizers yields a response that correctly lists the vetting-main
        // package as vetted on P2-S1, unvetted on P2-S2, and does not report the
        // status of P1-S1 or P1-S2 in the response at all.
        participant1JustLedger2Response <- participant1.listVettedPackages(
          ListVettedPackagesRequest(
            Some(PackageMetadataFilter(Seq(), Seq()).toProtoLAPI),
            Some(
              TopologyStateFilter(
                participantIds = Seq(participant2Id),
                synchronizerIds = Seq(sync1, sync2),
              )
            ),
            "",
            0,
          )
        )

        _ = {
          assertParticipantAndSynchronizerNotInResponse(
            participant1Id,
            sync1,
            participant1JustLedger2Response,
          )
          assertVettedOnParticipantAndSynchronizer(
            participant2Id,
            sync1,
            vettingMainPkgIdV2,
            participant1JustLedger2Response,
          )
          assertParticipantAndSynchronizerNotInResponse(
            participant1Id,
            sync2,
            participant1JustLedger2Response,
          )
          assertUnvettedOnParticipantAndSynchronizer(
            participant2Id,
            sync2,
            vettingMainPkgIdV2,
            participant1JustLedger2Response,
          )
        }

        // Test that restricting the query to both participants on one
        // synchronizer yields a response that correctly lists the vetting-main
        // package as vetted on P1-S2, unvetted on P2-S2, and does not report the
        // status of P1-S1 nor P2-S1 in the response at all.
        participant1JustSync2Response <- participant1.listVettedPackages(
          ListVettedPackagesRequest(
            Some(PackageMetadataFilter(Seq(), Seq()).toProtoLAPI),
            Some(
              TopologyStateFilter(
                participantIds = Seq(participant1Id, participant2Id),
                synchronizerIds = Seq(sync2),
              )
            ),
            "",
            0,
          )
        )

        _ = {
          assertParticipantAndSynchronizerNotInResponse(
            participant1Id,
            sync1,
            participant1JustSync2Response,
          )
          assertParticipantAndSynchronizerNotInResponse(
            participant2Id,
            sync1,
            participant1JustSync2Response,
          )
          assertVettedOnParticipantAndSynchronizer(
            participant1Id,
            sync2,
            vettingMainPkgIdV2,
            participant1JustSync2Response,
          )
          assertUnvettedOnParticipantAndSynchronizer(
            participant2Id,
            sync2,
            vettingMainPkgIdV2,
            participant1JustSync2Response,
          )
        }

        // Test that restricting the query to a given package will exclude
        // participant/synchronizer pairs that do not have that package vetted
        // from the response.
        participant1JustMainResponse <- participant1.listVettedPackages(
          ListVettedPackagesRequest(
            Some(PackageMetadataFilter(Seq(vettingMainPkgIdV2), Seq()).toProtoLAPI),
            Some(TopologyStateFilter(participantIds = Seq(), synchronizerIds = Seq())),
            "",
            0,
          )
        )

        _ = {
          assertVettedOnParticipantAndSynchronizer(
            participant1Id,
            sync1,
            vettingMainPkgIdV2,
            participant1JustMainResponse,
          )
          assertVettedOnParticipantAndSynchronizer(
            participant2Id,
            sync1,
            vettingMainPkgIdV2,
            participant1JustMainResponse,
          )
          assertVettedOnParticipantAndSynchronizer(
            participant1Id,
            sync2,
            vettingMainPkgIdV2,
            participant1JustMainResponse,
          )
          assertParticipantAndSynchronizerNotInResponse(
            participant2Id,
            sync2,
            participant1JustMainResponse,
          )
        }

        // Test that package filters and a node filters will intersect correctly.
        participant1JustMainAndSync2Response <- participant1.listVettedPackages(
          ListVettedPackagesRequest(
            Some(PackageMetadataFilter(Seq(vettingMainPkgIdV2), Seq()).toProtoLAPI),
            Some(TopologyStateFilter(participantIds = Seq(), synchronizerIds = Seq(sync2))),
            "",
            0,
          )
        )

        _ = {
          assertParticipantAndSynchronizerNotInResponse(
            participant1Id,
            sync1,
            participant1JustMainAndSync2Response,
          )
          assertParticipantAndSynchronizerNotInResponse(
            participant2Id,
            sync1,
            participant1JustMainAndSync2Response,
          )
          assertVettedOnParticipantAndSynchronizer(
            participant1Id,
            sync2,
            vettingMainPkgIdV2,
            participant1JustMainAndSync2Response,
          )
          assertParticipantAndSynchronizerNotInResponse(
            participant2Id,
            sync2,
            participant1JustMainAndSync2Response,
          )
        }

        _ <- unvetDARMains(participant1, Seq(VettingMainDar_2_0_0.path), Some(sync2))

        // Test that we can unvet a package on a specific participant/synchronizer
        // pair and have that reflected in a wildcard query.
        participant1AllResponseAfterUnvet <- participant1.listVettedPackages(
          ListVettedPackagesRequest(
            Some(PackageMetadataFilter(Seq(), Seq()).toProtoLAPI),
            Some(TopologyStateFilter(participantIds = Seq(), synchronizerIds = Seq())),
            "",
            0,
          )
        )

        _ = {
          assertVettedOnParticipantAndSynchronizer(
            participant1Id,
            sync1,
            vettingMainPkgIdV2,
            participant1AllResponseAfterUnvet,
          )
          assertVettedOnParticipantAndSynchronizer(
            participant2Id,
            sync1,
            vettingMainPkgIdV2,
            participant1AllResponseAfterUnvet,
          )
          assertUnvettedOnParticipantAndSynchronizer(
            participant1Id,
            sync2,
            vettingMainPkgIdV2,
            participant1AllResponseAfterUnvet,
          )
          assertUnvettedOnParticipantAndSynchronizer(
            participant2Id,
            sync2,
            vettingMainPkgIdV2,
            participant1AllResponseAfterUnvet,
          )
        }

        _ <- unvetAllDARMains(participant1)
        _ <- unvetAllDARMains(participant2)
      } yield ()
  })

  def requestAllPaged(
      size: Int = 0,
      token: String = "",
      participantIds: Seq[String] = Seq(),
      synchronizerIds: Seq[String] = Seq(),
  ) = ListVettedPackagesRequest(
    Some(PackageMetadataFilter(Seq(), Seq()).toProtoLAPI),
    Some(
      TopologyStateFilter(
        participantIds = participantIds,
        synchronizerIds = synchronizerIds,
      )
    ),
    token,
    size,
  )

  def vettedPackagesAreSorted(results: Seq[VettedPackages], clue: String): Assertion = {
    val keys = results.map(packages => packages.synchronizerId -> packages.participantId)
    keys.sorted should equal(
      keys
    ) withClue (clue + " is not sorted")
  }

  test(
    "PVListVettedPackagesPagination",
    "Listing packages with pagination works.",
    allocate(NoParties, NoParties).expectingMinimumNumberOfSynchronizers(2),
    runConcurrently = false,
  )(implicit ec => {
    case Participants(Participant(participant1, _), Participant(participant2, _)) =>
      for {
        participant1Id <- participant1.getParticipantId()
        participant2Id <- participant2.getParticipantId()
        sync1 <- getSynchronizerId(participant1, syncIndex = 1)
        sync2 <- getSynchronizerId(participant1, syncIndex = 2)

        // Filter vettedPackages results to only those that are about the two
        // synchronizers and two participants that are part of this test.
        filterToLocalNodes = { (results: Seq[VettedPackages]) =>
          results.filter { result =>
            val hasSync = Seq(sync1, sync2).contains(result.synchronizerId)
            val hasParticipant = Seq(participant1Id, participant2Id).contains(result.participantId)
            hasSync && hasParticipant
          }
        }

        allPairsHaveLengthN = { (results: Seq[VettedPackages], expectedLength: Long) =>
          filterToLocalNodes(results) should have length expectedLength
        }

        // Previous test runs might mean other participant/synchronizer pairs
        // will be left over in vetting states. This filters to just pairs from
        // the changes that we've made, and asserts that they've vetted
        // vetting-main 2.0.0
        allPairsVetMain = { (results: Seq[VettedPackages]) =>
          forAll(filterToLocalNodes(results)) { result =>
            assertVettedPackagesHasPkgIds(
              result,
              hasPkgIds = Seq(vettingMainPkgIdV2),
              hasNotPkgIds = Seq(),
              alternativeSynchronizer = Some(result.synchronizerId),
              alternativeParticipant = Some(result.participantId),
            )
          }
        }

        _ <- participant1.uploadDarFile(
          UploadDarFileRequest(
            Dars.read(VettingMainDar_2_0_0.path),
            "",
            DontVetAnyPackages.toProto,
            "",
          )
        )
        _ <- participant2.uploadDarFile(
          UploadDarFileRequest(
            Dars.read(VettingMainDar_2_0_0.path),
            "",
            DontVetAnyPackages.toProto,
            "",
          )
        )

        _ <- vetAllInDar(participant2, VettingMainDar_2_0_0.path, Some(sync2))
        _ <- vetAllInDar(participant1, VettingMainDar_2_0_0.path, Some(sync2))
        _ <- vetAllInDar(participant2, VettingMainDar_2_0_0.path, Some(sync1))
        _ <- vetAllInDar(participant1, VettingMainDar_2_0_0.path, Some(sync1))

        // Requesting page of size 0 means requesting server's default page
        // size, which is at least 4
        requestZero <- participant1.listVettedPackages(requestAllPaged(size = 0))
        _ = allPairsHaveLengthN(requestZero.vettedPackages, 4)
        _ = allPairsVetMain(requestZero.vettedPackages)
        _ = vettedPackagesAreSorted(requestZero.vettedPackages, "requestZero")

        // Requesting page of size 1 gets one result
        requestOne <- participant1.listVettedPackages(requestAllPaged(size = 1))
        _ = allPairsHaveLengthN(requestOne.vettedPackages, 1)

        // Requesting page with token from last request gives new result
        requestAnotherOne <- participant1.listVettedPackages(
          requestAllPaged(size = 1, token = requestOne.nextPageToken)
        )
        _ = allPairsHaveLengthN(requestAnotherOne.vettedPackages, 1)

        // Requesting page of size larger than remaining items returns all
        // remaining items. We assume here that maxPageSize exceeds the number
        // of synchronizer/participant pairs.
        maxPageSize = participant1.features.packageFeature.maxVettedPackagesPageSize
        requestRemainder <- participant1.listVettedPackages(
          requestAllPaged(size = maxPageSize, token = requestAnotherOne.nextPageToken)
        )
        _ = requestRemainder.vettedPackages.length should be < maxPageSize

        // All chained requests should be sorted
        chainedRequests =
          requestOne.vettedPackages ++ requestAnotherOne.vettedPackages ++ requestRemainder.vettedPackages
        _ = allPairsHaveLengthN(requestZero.vettedPackages, 4)
        _ = allPairsVetMain(chainedRequests)
        _ = vettedPackagesAreSorted(chainedRequests, "chainedRequests")

        // Requesting page with specific synchronizer adheres to that synchronizer
        requestJustSync1Step1 <- participant1.listVettedPackages(
          requestAllPaged(size = 1, synchronizerIds = Seq(sync1))
        )
        requestJustSync1Step2 <- participant1.listVettedPackages(
          requestAllPaged(
            size = 1,
            synchronizerIds = Seq(sync1),
            token = requestJustSync1Step1.nextPageToken,
          )
        )
        requestJustSync1Step3 <- participant1.listVettedPackages(
          requestAllPaged(
            size = 1,
            synchronizerIds = Seq(sync1),
            token = requestJustSync1Step2.nextPageToken,
          )
        )
        _ = allPairsHaveLengthN(requestJustSync1Step3.vettedPackages, 0)

        requestJustSync1 =
          requestJustSync1Step1.vettedPackages ++ requestJustSync1Step2.vettedPackages ++ requestJustSync1Step3.vettedPackages
        _ = allPairsHaveLengthN(requestJustSync1, 2)
        _ = allPairsVetMain(requestJustSync1)
        _ = vettedPackagesAreSorted(requestJustSync1, "requestJustSync1")

        // Requesting page with specific participant adheres to that participant
        requestJustPtcpt1Step1 <- participant1.listVettedPackages(
          requestAllPaged(size = 1, participantIds = Seq(participant1Id))
        )
        requestJustPtcpt1Step2 <- participant1.listVettedPackages(
          requestAllPaged(
            size = 1,
            participantIds = Seq(participant1Id),
            token = requestJustPtcpt1Step1.nextPageToken,
          )
        )
        requestJustPtcpt1Step3 <- participant1.listVettedPackages(
          requestAllPaged(
            size = 1,
            participantIds = Seq(participant1Id),
            token = requestJustPtcpt1Step2.nextPageToken,
          )
        )
        _ = allPairsHaveLengthN(requestJustPtcpt1Step3.vettedPackages, 0)

        requestJustPtcpt1 =
          requestJustPtcpt1Step1.vettedPackages ++ requestJustPtcpt1Step2.vettedPackages ++ requestJustPtcpt1Step3.vettedPackages
        _ = allPairsHaveLengthN(requestJustPtcpt1, 2)
        _ = allPairsVetMain(requestJustPtcpt1)
        _ = vettedPackagesAreSorted(requestJustPtcpt1, "requestJustPtcpt1")

        // Requesting page with specific participant and synchronizer adheres to that participant and synchronizer
        requestJustPairStep1 <- participant1.listVettedPackages(
          requestAllPaged(
            size = 1,
            synchronizerIds = Seq(sync1),
            participantIds = Seq(participant1Id),
          )
        )
        requestJustPairStep2 <- participant1.listVettedPackages(
          requestAllPaged(
            size = 1,
            synchronizerIds = Seq(sync1),
            participantIds = Seq(participant1Id),
            token = requestJustPairStep1.nextPageToken,
          )
        )
        _ = allPairsHaveLengthN(requestJustPairStep2.vettedPackages, 0)

        requestJustPair = requestJustPairStep1.vettedPackages ++ requestJustPairStep2.vettedPackages
        _ = allPairsHaveLengthN(requestJustPair, 1)
        _ = allPairsVetMain(requestJustPair)
        _ = vettedPackagesAreSorted(requestJustPair, "requestJustPair")

        // Requesting page of size below 0 results in an error
        _ <- participant1
          .listVettedPackages(requestAllPaged(size = -1))
          .mustFailWith(
            "Requesting page of size below 0 results in an error",
            ProtoDeserializationFailure,
          )

        // Requesting page of size below 0 results in an error
        _ <- participant1
          .listVettedPackages(requestAllPaged(size = -1))
          .mustFailWith(
            "Requesting page of size below 0 results in an error",
            ProtoDeserializationFailure,
          )

        // Requesting page of size more than the maximum server page size
        // results in an error
        _ <- participant1
          .listVettedPackages(requestAllPaged(size = maxPageSize + 1))
          .mustFailWith(
            "Requesting page of size more than the maximum server page size results in an error",
            ProtoDeserializationFailure,
          )

        // Requesting invalid pageToken results in an error
        _ <- participant1
          .listVettedPackages(requestAllPaged(size = 1, token = "INVALID_PAGE_TOKEN"))
          .mustFailWith(
            "Requesting invalid pageToken results in an error",
            ProtoDeserializationFailure,
          )

        _ <- unvetAllDARMains(participant1)
        _ <- unvetAllDARMains(participant2)
      } yield ()
  })

  // TODO(#28384): Re-enable these tests when
  // LedgerApiConformanceMultiSynchronizerTest is able to have participants with
  // different synchronizers.
  /*
  test(
    "PVListVettedPackagesAsymmetricSynchronizer",
    "Listing packages on a per-synchronizer and per-participant basis",
    allocate(NoParties, NoParties)
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant1, _), Participant(participant2, _)) =>
    for {
      _ <- unvetAllDARMains(participant1)
      _ <- unvetAllDARMains(participant2)

      participant1Id <- participant1.getParticipantId()
      participant2Id <- participant2.getParticipantId()
      sync1 <- getSynchronizerId(participant1, syncIndex = 1)
      sync2 <- getSynchronizerId(participant1, syncIndex = 2)

      _ <- participant1.uploadDarFile(
        UploadDarFileRequest(
          Dars.read(VettingMainDar_2_0_0.path),
          "",
          DontVetAnyPackages.toProto,
          "",
        )
      )
      _ <- participant2.uploadDarFile(
        UploadDarFileRequest(
          Dars.read(VettingMainDar_2_0_0.path),
          "",
          DontVetAnyPackages.toProto,
          "",
        )
      )
      _ <- participant2.uploadDarFile(
        UploadDarFileRequest(
          Dars.read(VettingAltDar.path),
          "",
          DontVetAnyPackages.toProto,
          "",
        )
      )

      _ <- vetAllInDar(participant1, VettingMainDar_2_0_0.path, Some(sync1))
      _ <- vetAllInDar(participant2, VettingMainDar_2_0_0.path, Some(sync1))
      _ <- vetAllInDar(participant1, VettingMainDar_2_0_0.path, Some(sync2))

      // Assert that participant1 can list vetting for both participants because
      // it is connected to both synchronizers.
      participant1AllResponse <- participant1.listVettedPackages(
        ListVettedPackagesRequest(
          Some(PackageMetadataFilter(Seq(), Seq()).toProtoLAPI),
          Some(TopologyStateFilter(participantIds = Seq(), synchronizerIds = Seq())),
          "",
          0,
        )
      )

      _ = {
        assertVettedOnParticipantAndSynchronizer(
          participant1Id,
          sync1,
          vettingMainPkgIdV2,
          participant1AllResponse,
        )
        assertVettedOnParticipantAndSynchronizer(
          participant2Id,
          sync1,
          vettingMainPkgIdV2,
          participant1AllResponse,
        )
        assertVettedOnParticipantAndSynchronizer(
          participant1Id,
          sync2,
          vettingMainPkgIdV2,
          participant1AllResponse,
        )
        assertParticipantAndSynchronizerNotInResponse(
          participant2Id,
          sync2,
          participant1AllResponse,
        )
      }

      // Assert that participant2 can only list vetting for vetting states on
      // sync1, because it is not connected to sync2.
      participant2AllResponse <- participant2.listVettedPackages(
        ListVettedPackagesRequest(
          Some(PackageMetadataFilter(Seq(), Seq()).toProtoLAPI),
          Some(TopologyStateFilter(participantIds = Seq(), synchronizerIds = Seq())),
          "",
          0,
        )
      )

      _ = {
        assertVettedOnParticipantAndSynchronizer(
          participant1Id,
          sync1,
          vettingMainPkgIdV2,
          participant1AllResponse,
        )
        assertVettedOnParticipantAndSynchronizer(
          participant2Id,
          sync1,
          vettingMainPkgIdV2,
          participant1AllResponse,
        )
        assertParticipantAndSynchronizerNotInResponse(
          participant1Id,
          sync2,
          participant1AllResponse,
        )
        assertParticipantAndSynchronizerNotInResponse(
          participant2Id,
          sync2,
          participant1AllResponse,
        )
      }

      _ <- unvetAllDARMains(participant1)
      _ <- unvetAllDARMains(participant2)
    } yield ()
  })
   */

  implicit val mainTSimple1_0_0Companion: ContractCompanion.WithoutKey[
    MainTSimple_1_0_0.Contract,
    MainTSimple_1_0_0.ContractId,
    MainTSimple_1_0_0,
  ] = MainTSimple_1_0_0.COMPANION

  test(
    "PVUnvetPackageWithActiveContracts",
    "Unvet packages that have an active contract",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(participant, Seq(party))) =>
    for {
      _ <- setNodeIds(participant)
      _ <- participant.uploadDarFile(
        UploadDarFileRequest(
          Dars.read(VettingMainDar_1_0_0.path),
          "",
          DontVetAnyPackages.toProto,
          "",
        )
      )

      _ <- participant.uploadDarFile(
        UploadDarFileRequest(
          Dars.read(VettingMainDar_2_0_0.path),
          "",
          DontVetAnyPackages.toProto,
          "",
        )
      )

      vetMain <- vetAllInDar(participant, VettingMainDar_1_0_0.path)

      _ <- participant.create(party, new MainTSimple_1_0_0(party))

      unvetMain <- unvetDARMains(
        participant,
        Seq(VettingMainDar_1_0_0.path),
      )

      _ <- unvetAllDARMains(participant)
    } yield {
      vetMain.getNewVettedPackages.packages.map(_.packageId) should contain(vettingMainPkgIdV1)
      unvetMain.getNewVettedPackages.packages.map(_.packageId) should not contain vettingMainPkgIdV1
    }
  })
}
