// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  NoParties,
  Participant,
  Participants,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{
  Dars,
  LedgerTestSuite,
  TestConstraints,
  VettingAltDar,
  VettingDepDar,
  VettingMainDar_1_0_0,
  VettingMainDar_2_0_0,
  VettingMainDar_Split_Lineage_2_0_0,
}
import com.daml.ledger.api.v2.admin.package_management_service.{
  UpdateVettedPackagesRequest,
  UploadDarFileRequest,
  VettedPackagesChange,
  VettedPackagesRef,
}
import com.daml.ledger.api.v2.package_reference.VettedPackages
import com.daml.ledger.api.v2.package_service.{
  ListVettedPackagesRequest,
  ListVettedPackagesResponse,
  TopologyStateFilter,
}
import com.daml.ledger.test.java.vetting_alt.alt.AltT
import com.daml.ledger.test.java.vetting_dep.dep.DepT
import com.daml.ledger.test.java.vetting_main_1_0_0.main.MainT as MainT_1_0_0
import com.daml.ledger.test.java.vetting_main_2_0_0.main.MainT as MainT_2_0_0
import com.daml.ledger.test.java.vetting_main_split_lineage_2_0_0.main.DifferentMainT as MainT_Split_Lineage_2_0_0
import com.digitalasset.canton.ledger.api.{
  DontVetAnyPackages,
  PackageMetadataFilter,
  PriorTopologySerialExists,
}
import com.digitalasset.canton.participant.admin.CantonPackageServiceError
import com.digitalasset.canton.topology.TopologyManagerError.ParticipantTopologyManagerError
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.timestamp.Timestamp
import org.scalatest.Inside.inside
import org.scalatest.Inspectors.*
import org.scalatest.compatible.Assertion
import org.scalatest.matchers.should.Matchers.*

import java.util.concurrent.atomic.AtomicReference
import java.util.zip.ZipInputStream
import scala.concurrent.{ExecutionContext, Future}

class VettingIT extends LedgerTestSuite {
  private val vettingDepPkgId = Ref.PackageId.assertFromString(DepT.PACKAGE_ID)
  private val vettingAltPkgId = Ref.PackageId.assertFromString(AltT.PACKAGE_ID)
  private val vettingMainPkgIdV1 = Ref.PackageId.assertFromString(MainT_1_0_0.PACKAGE_ID)
  private val vettingMainPkgIdV2 = Ref.PackageId.assertFromString(MainT_2_0_0.PACKAGE_ID)
  private val vettingMainPkgIdV2SplitLineage =
    Ref.PackageId.assertFromString(MainT_Split_Lineage_2_0_0.PACKAGE_ID)

  private val vettingDepName = "vetting-dep"
  private val vettingMainName = "vetting-main"

  private val synchronizer1Id: AtomicReference[Option[String]] =
    new AtomicReference[Option[String]](None)
  private def synchronizerIdOrFail = synchronizer1Id
    .get()
    .getOrElse(throw new IllegalStateException("synchronizerId not yet discovered"))

  private def assertListResponseHasPkgIds(
      response: ListVettedPackagesResponse,
      hasPkgIds: Seq[Ref.PackageId],
      hasNotPkgIds: Seq[Ref.PackageId],
  ): Assertion =
    inside(response) { case ListVettedPackagesResponse(Seq(vettedPackages), _) =>
      assertVettedPackagesHasPkgIds(vettedPackages, hasPkgIds, hasNotPkgIds)
    }

  private def assertSomeVettedPackagesHasPkgIds(
      mbVettedPackages: Option[VettedPackages],
      hasPkgIds: Seq[Ref.PackageId],
      hasNotPkgIds: Seq[Ref.PackageId],
  ): Assertion =
    inside(mbVettedPackages) { case Some(vettedPackages) =>
      assertVettedPackagesHasPkgIds(vettedPackages, hasPkgIds, hasNotPkgIds)
    }

  private def assertVettedPackagesHasPkgIds(
      vettedPackages: VettedPackages,
      hasPkgIds: Seq[Ref.PackageId],
      hasNotPkgIds: Seq[Ref.PackageId],
  ): Assertion = {
    val allPkgIds = vettedPackages.packages.map(_.packageId)
    forAll(hasPkgIds) { pkgId =>
      allPkgIds should contain(pkgId)
    }
    forAll(hasNotPkgIds) { pkgId =>
      allPkgIds should not contain pkgId
    }
  }

  private def assertListResponseHasPkgIdWithBounds(
      response: ListVettedPackagesResponse,
      targetPkgId: Ref.PackageId,
      expectedLowerBound: Option[Timestamp],
      expectedUpperBound: Option[Timestamp],
  ): Assertion =
    inside(response) { case ListVettedPackagesResponse(Seq(vettedPackages), _) =>
      val matching = vettedPackages.packages.find(_.packageId == targetPkgId)
      inside(matching) { case Some(vetted) =>
        vetted.validFromInclusive shouldBe expectedLowerBound
        vetted.validUntilExclusive shouldBe expectedUpperBound
      }
    }

  private def assertResponseHasAllVersions(response: ListVettedPackagesResponse): Assertion =
    assertListResponseHasPkgIds(
      response = response,
      hasPkgIds = Seq(vettingDepPkgId, vettingMainPkgIdV2, vettingMainPkgIdV1),
      hasNotPkgIds = Seq(vettingAltPkgId),
    )

  private def assertResponseHasV2(response: ListVettedPackagesResponse): Assertion =
    assertListResponseHasPkgIds(
      response = response,
      hasPkgIds = Seq(vettingDepPkgId, vettingMainPkgIdV2),
      hasNotPkgIds = Seq(vettingAltPkgId, vettingMainPkgIdV1),
    )

  private def assertResponseHasDep(response: ListVettedPackagesResponse): Assertion =
    assertListResponseHasPkgIds(
      response = response,
      hasPkgIds = Seq(vettingDepPkgId),
      hasNotPkgIds = Seq(vettingMainPkgIdV2, vettingAltPkgId),
    )

  private def assertResponseHasNothing(response: ListVettedPackagesResponse): Assertion =
    assertListResponseHasPkgIds(
      response = response,
      hasPkgIds = Seq(),
      hasNotPkgIds = Seq(vettingDepPkgId, vettingMainPkgIdV1, vettingMainPkgIdV2, vettingAltPkgId),
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
        TopologyStateFilter(participantIds = Seq.empty, synchronizerIds = Seq(synchronizerIdOrFail))
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
  ): UpdateVettedPackagesRequest =
    UpdateVettedPackagesRequest(
      operations.map(VettedPackagesChange(_)),
      dryRun,
      synchronizerIdOrFail,
      Some(PriorTopologySerialExists(0).toProtoLAPI),
    )

  private def changeOpRequest(
      operation: VettedPackagesChange.Operation,
      dryRun: Boolean = false,
  ): UpdateVettedPackagesRequest =
    changeOpsRequest(Seq(operation), dryRun)

  private def vetPkgIdsRequest(
      pkgIds: Seq[String],
      dryRun: Boolean = false,
  ): UpdateVettedPackagesRequest =
    changeOpsRequest(
      Seq(refsToVetOp(pkgIds.map((pkgId: String) => VettedPackagesRef(pkgId, "", "")))),
      dryRun,
    )

  private def vetPkgsMatchingRef(
      ledger: ParticipantTestContext,
      ref: VettedPackagesRef,
      newValidFromInclusive: Option[Timestamp] = None,
      newValidUntilExclusive: Option[Timestamp] = None,
  )(implicit
      ec: ExecutionContext
  ): Future[Unit] =
    ledger
      .updateVettedPackages(
        changeOpRequest(
          refsToVetOp(
            Seq(ref),
            newValidFromInclusive,
            newValidUntilExclusive,
          )
        )
      )
      .map(_ => ())

  private def vetAllInDar(ledger: ParticipantTestContext, darName: String)(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    val allPackageIds = DarDecoder
      .readArchive(
        darName,
        new ZipInputStream(getClass.getClassLoader.getResourceAsStream(darName)),
      )
      .toOption
      .get
      .all
      .map(_._1)
    ledger
      .updateVettedPackages(
        vetPkgIdsRequest(allPackageIds.map(_.toString))
      )
      .map(_ => ())
  }

  private def opDARMains(
      op: Seq[VettedPackagesRef] => VettedPackagesChange.Operation,
      ledger: ParticipantTestContext,
      darNames: Seq[String],
  )(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    val mainPackageIds = darNames.map((darName: String) =>
      DarDecoder
        .readArchive(
          darName,
          new ZipInputStream(getClass.getClassLoader.getResourceAsStream(darName)),
        )
        .toOption
        .get
        .main
        ._1
    )

    ledger
      .updateVettedPackages(
        changeOpsRequest(
          Seq(
            op(
              mainPackageIds.map((pkgId: Ref.PackageId) =>
                VettedPackagesRef(pkgId.toString, "", "")
              )
            )
          ),
          false,
        )
      )
      .map(_ => ())
  }

  private def unvetDARMains(ledger: ParticipantTestContext, darNames: Seq[String])(implicit
      ec: ExecutionContext
  ): Future[Unit] =
    opDARMains(refsToUnvetOp, ledger, darNames)

  private def vetDARMains(ledger: ParticipantTestContext, darNames: Seq[String])(implicit
      ec: ExecutionContext
  ): Future[Unit] =
    opDARMains(refsToVetOp(_), ledger, darNames)

  private def unvetAllDARMains(
      ledger: ParticipantTestContext
  )(implicit ec: ExecutionContext): Future[Unit] =
    unvetDARMains(
      ledger,
      Seq(
        VettingDepDar.path,
        VettingMainDar_1_0_0.path,
        VettingMainDar_2_0_0.path,
        VettingAltDar.path,
      ),
    )
      .map(_ => ())

  private def setSynchronizerId(
      ledger: ParticipantTestContext
  )(implicit ec: ExecutionContext): Future[Unit] =
    ledger.connectedSynchronizers().map { connected =>
      connected.find(_.startsWith("synchronizer1")).foreach { syncId =>
        synchronizer1Id.set(Some(syncId))
      }
    }

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
  )(implicit ec => { case Participants(Participant(ledger, _)) =>
    for {
      _ <- setSynchronizerId(ledger)
      _ <- ledger.uploadDarFileAndVetOnConnectedSynchronizers(Dars.read(VettingDepDar.path))
      _ <- ledger.uploadDarFileAndVetOnConnectedSynchronizers(Dars.read(VettingMainDar_1_0_0.path))
      _ <- ledger.uploadDarFileAndVetOnConnectedSynchronizers(Dars.read(VettingMainDar_2_0_0.path))
      _ <- ledger.uploadDarFile(
        uploadDarFileDontVetRequest(VettingAltDar.path)
      )

      allResponse <- ledger.listVettedPackages(listAllRequest)
      depNameResponse <- ledger.listVettedPackages(listNamesRequest(Seq(vettingDepName)))
      bothNameResponse <- ledger.listVettedPackages(
        listNamesRequest(Seq(vettingDepName, vettingMainName))
      )
      depPkgIdResponse <- ledger.listVettedPackages(listPkgIdsRequest(Seq(vettingDepPkgId)))
      bothPkgIdResponse <- ledger.listVettedPackages(
        listPkgIdsRequest(Seq(vettingDepPkgId, vettingMainPkgIdV2))
      )
      depPkgIdAndNameResponse <- ledger.listVettedPackages(
        listPackagesRequest(Seq(vettingDepPkgId), Seq(vettingDepName))
      )
      bothPkgIdAndNamesResponse <- ledger.listVettedPackages(
        listPackagesRequest(
          Seq(vettingDepPkgId, vettingMainPkgIdV2),
          Seq(vettingDepName, vettingMainName),
        )
      )
      commonPrefixResponse <- ledger.listVettedPackages(listNamesRequest(Seq("vetting-")))
      disjointPkgIdAndNameResponse <- ledger.listVettedPackages(
        listPackagesRequest(Seq(vettingDepPkgId), Seq(vettingMainName))
      )

      _ <- unvetAllDARMains(ledger)
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
  )(implicit ec => { case Participants(Participant(ledger, _)) =>
    for {
      _ <- setSynchronizerId(ledger)
      _ <- ledger.uploadDarFileAndVetOnConnectedSynchronizers(
        Dars.read(VettingMainDar_2_0_0.path)
      ) // Should vet vetting-dep dependency
      _ <- ledger.uploadDarFile(
        uploadDarFileDontVetRequest(VettingAltDar.path)
      )

      allResponse <- ledger.listVettedPackages(listAllRequest)
      _ <- unvetAllDARMains(ledger)
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
    )(implicit ec => { case Participants(Participant(ledger, _)) =>
      for {
        _ <- setSynchronizerId(ledger)
        _ <- ledger.uploadDarFile(
          uploadDarFileDontVetRequest(VettingDepDar.path)
        )
        _ <- ledger.uploadDarFile(
          uploadDarFileDontVetRequest(VettingMainDar_1_0_0.path)
        )
        _ <- ledger.uploadDarFile(
          uploadDarFileDontVetRequest(VettingMainDar_2_0_0.path)
        )
        _ <- ledger.uploadDarFile(
          uploadDarFileDontVetRequest(VettingAltDar.path)
        )
        _ <- unvetAllDARMains(ledger)
        _ <- act(ec)(ledger)
        _ <- unvetAllDARMains(ledger)
      } yield {
        ()
      }
    })

  updateTest(
    "PVUpdateVetDepThenV2Succeeds",
    "Successfully vet everything in vetting-dep and then just the main package from vetting-main",
    implicit ec =>
      (ledger: ParticipantTestContext) =>
        for {
          _ <- vetAllInDar(ledger, VettingDepDar.path)
          _ <- vetDARMains(ledger, Seq(VettingMainDar_2_0_0.path))

          allResponse <- ledger.listVettedPackages(listAllRequest)
        } yield assertResponseHasV2(allResponse),
  )

  updateTest(
    "PVUpdateUnvetV2Succeeds",
    "Successfully vet everything in vetting-main then just unvet the main package from vetting-main",
    implicit ec =>
      (ledger: ParticipantTestContext) =>
        for {
          _ <- vetAllInDar(ledger, VettingMainDar_2_0_0.path)
          _ <- unvetDARMains(ledger, Seq(VettingMainDar_2_0_0.path))

          unvetTestAllResponse <- ledger.listVettedPackages(listAllRequest)
        } yield assertResponseHasDep(unvetTestAllResponse),
  )

  updateTest(
    "PVUpdateVetTwoPackagesAtATimeSucceeds",
    "Successfully vet both vetting-dep and vetting-main in one update",
    implicit ec =>
      (ledger: ParticipantTestContext) =>
        for {
          _ <- ledger.updateVettedPackages(
            vetPkgIdsRequest(Seq(vettingMainPkgIdV2, vettingDepPkgId))
          )

          vetMainAndDepResponse <- ledger.listVettedPackages(listAllRequest)
        } yield assertResponseHasV2(vetMainAndDepResponse),
  )

  updateTest(
    "PVUpdateVetMultipleByNameFails",
    "Successfully vet multiple packages by name",
    implicit ec =>
      (ledger: ParticipantTestContext) =>
        for {
          _ <- vetAllInDar(ledger, VettingDepDar.path)
          _ <- vetPkgsMatchingRef(ledger, VettedPackagesRef("", vettingMainName, ""))
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
      (ledger: ParticipantTestContext) =>
        for {
          _ <- vetAllInDar(ledger, VettingDepDar.path)
          _ <- vetPkgsMatchingRef(ledger, VettedPackagesRef("", "nonexistent-name", ""))
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
      (ledger: ParticipantTestContext) =>
        for {
          _ <- vetAllInDar(ledger, VettingDepDar.path)
          _ <- vetPkgsMatchingRef(
            ledger,
            VettedPackagesRef("", vettingMainName, ("2.0.0")),
          )
          vetOnlyV2ByNameAndVersionResponse <- ledger.listVettedPackages(listAllRequest)
        } yield assertResponseHasV2(vetOnlyV2ByNameAndVersionResponse),
  )

  updateTest(
    "PVUpdateVetByNameAndNonexistentVersion",
    "Fail when trying to vet nonexistent version of a package name that exists",
    implicit ec =>
      (ledger: ParticipantTestContext) =>
        for {
          _ <- vetAllInDar(ledger, VettingDepDar.path)
          _ <- vetPkgsMatchingRef(
            ledger,
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
      (ledger: ParticipantTestContext) =>
        for {
          _ <- vetAllInDar(ledger, VettingDepDar.path)
          _ <- vetPkgsMatchingRef(
            ledger,
            VettedPackagesRef(
              vettingMainPkgIdV2,
              vettingMainName,
              "2.0.0",
            ),
          )
          vetOnlyV2ByAllResponse <- ledger.listVettedPackages(listAllRequest)
        } yield assertResponseHasV2(vetOnlyV2ByAllResponse),
  )

  updateTest(
    "PVUpdateVetByIdWithWrongName",
    "Fail to vet a package by ID when paired with the wrong name",
    implicit ec =>
      (ledger: ParticipantTestContext) =>
        for {
          _ <- vetAllInDar(ledger, VettingDepDar.path)
          _ <- vetPkgsMatchingRef(
            ledger,
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
      (ledger: ParticipantTestContext) =>
        for {
          _ <- vetAllInDar(ledger, VettingDepDar.path)
          _ <- vetPkgsMatchingRef(
            ledger,
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

  test(
    "PVListVettedPackagesNothingVetted",
    "Listing vetted packages returns nothing when uploading DARs without vetting them",
    allocate(NoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, _)) =>
    for {
      _ <- setSynchronizerId(ledger)
      _ <- ledger.uploadDarFile(
        uploadDarFileDontVetRequest(VettingDepDar.path)
      )
      _ <- ledger.uploadDarFile(
        uploadDarFileDontVetRequest(VettingMainDar_2_0_0.path)
      )
      _ <- ledger.uploadDarFile(
        uploadDarFileDontVetRequest(VettingAltDar.path)
      )

      allResponse <- ledger.listVettedPackages(listAllRequest)

      _ <- unvetAllDARMains(ledger)
    } yield {
      assertResponseHasNothing(allResponse)
    }
  })

  test(
    "PVDryRun",
    "Vetting with Dry Run returns the expected changes, but does not commit them",
    allocate(NoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, _)) =>
    for {
      _ <- setSynchronizerId(ledger)
      _ <- ledger.uploadDarFile(
        uploadDarFileDontVetRequest(VettingMainDar_2_0_0.path)
      )
      _ <- ledger.uploadDarFile(
        uploadDarFileDontVetRequest(VettingAltDar.path)
      )
      dryRunUpdateResponse <- ledger.updateVettedPackages(
        vetPkgIdsRequest(
          pkgIds = Seq(vettingAltPkgId),
          dryRun = true,
        )
      )

      listAllResponse <- ledger.listVettedPackages(listAllRequest)
      _ <- unvetAllDARMains(ledger)
    } yield {
      assertSomeVettedPackagesHasPkgIds(
        mbVettedPackages = dryRunUpdateResponse.pastVettedPackages,
        hasPkgIds = Seq(),
        hasNotPkgIds = Seq(vettingDepPkgId, vettingMainPkgIdV2, vettingAltPkgId),
      )

      assertSomeVettedPackagesHasPkgIds(
        mbVettedPackages = dryRunUpdateResponse.newVettedPackages,
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
  )(implicit ec => { case Participants(Participant(ledger, _)) =>
    for {
      _ <- setSynchronizerId(ledger)
      _ <- ledger.uploadDarFile(
        uploadDarFileDontVetRequest(VettingMainDar_1_0_0.path)
      )
      _ <- ledger.uploadDarFile(
        uploadDarFileDontVetRequest(VettingAltDar.path)
      )
      _ <- vetAllInDar(ledger, VettingDepDar.path)

      _ <- vetPkgsMatchingRef(
        ledger,
        VettedPackagesRef(vettingMainPkgIdV2, "", ""),
        Some(Timestamp.of(0, 0)),
        Some(Timestamp.of(1, 0)),
      )

      listAfterBounds1 <- ledger.listVettedPackages(listAllRequest)

      _ <- ledger.updateVettedPackages(
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

      listAfterBounds2 <- ledger.listVettedPackages(listAllRequest)

      _ <- unvetAllDARMains(ledger)
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
    "PVCheckUpgradeInvariants",
    "Upgrade invariants are checked, including during dry run",
    allocate(NoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, _)) =>
    for {
      _ <- setSynchronizerId(ledger)
      _ <- ledger.uploadDarFile(
        uploadDarFileDontVetRequest(VettingMainDar_2_0_0.path)
      )
      _ <- ledger.uploadDarFile(
        uploadDarFileDontVetRequest(VettingMainDar_Split_Lineage_2_0_0.path)
      )

      // Dry-run vetting without dependencies (should fail)
      _ <- ledger
        .updateVettedPackages(vetPkgIdsRequest(pkgIds = Seq(vettingMainPkgIdV2), dryRun = true))
        .mustFailWith(
          "Vetting a package without its dependencies in a dry run should give TOPOLOGY_DEPENDENCIES_NOT_VETTED",
          ParticipantTopologyManagerError.DependenciesNotVetted,
        )

      // Vet without dependencies (should fail)
      _ <- ledger
        .updateVettedPackages(
          vetPkgIdsRequest(Seq(vettingMainPkgIdV2))
        )
        .mustFailWith(
          "Vetting a package without its dependencies should give TOPOLOGY_DEPENDENCIES_NOT_VETTED",
          ParticipantTopologyManagerError.DependenciesNotVetted,
        )

      _ <- vetAllInDar(ledger, VettingDepDar.path)

      // Vet two packages with the same version (should fail)
      _ <- ledger
        .updateVettedPackages(
          vetPkgIdsRequest(Seq(vettingMainPkgIdV2, vettingMainPkgIdV2SplitLineage))
        )
        .mustFailWith(
          "Update should fail to vet package with same name and version with KNOWN_PACKAGE_VERSION error",
          ParticipantTopologyManagerError.UpgradeVersion,
        )

      // Vet a package while unvetting its dependencies (should fail)
      _ <- ledger
        .updateVettedPackages(
          changeOpsRequest(
            Seq(
              refsToVetOp(
                Seq(VettedPackagesRef(vettingMainPkgIdV2, "", ""))
              ),
              refsToUnvetOp(
                Seq(VettedPackagesRef(vettingDepPkgId, "", ""))
              ),
            )
          )
        )
        .mustFailWith(
          "Vetting a package while unvetting its dependencies should give TOPOLOGY_DEPENDENCIES_NOT_VETTED",
          ParticipantTopologyManagerError.DependenciesNotVetted,
        )

      _ <- unvetAllDARMains(ledger)
    } yield ()
  })

  test(
    "PVValidateDarCheckUpgradeInvariants",
    "Upgrade invariants are checked, including during validate dar request",
    allocate(NoParties),
    runConcurrently = false,
    limitation = TestConstraints.GrpcOnly(reason = "ValidateDarFile is not available in JSON API"),
  )(implicit ec => { case Participants(Participant(ledger, _)) =>
    for {
      _ <- ledger.uploadDarFileAndVetOnConnectedSynchronizers(Dars.read(VettingMainDar_2_0_0.path))
      _ <- ledger
        .validateDarFile(Dars.read(VettingMainDar_Split_Lineage_2_0_0.path))
        .mustFailWith(
          "Update should fail to vet package with same name and version with KNOWN_PACKAGE_VERSION error",
          ParticipantTopologyManagerError.UpgradeVersion,
        )
      _ <- unvetAllDARMains(ledger)
    } yield ()
  })
}
