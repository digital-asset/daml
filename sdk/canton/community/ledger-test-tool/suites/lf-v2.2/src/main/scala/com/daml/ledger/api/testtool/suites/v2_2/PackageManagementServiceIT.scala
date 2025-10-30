// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.{
  LedgerTestSuite,
  PackageManagementTestDar,
  TestConstraints,
}
import com.daml.ledger.test.java.package_management.packagemanagementtest.PackageManagementTestTemplate
import com.digitalasset.canton.ledger.error.PackageServiceErrors
import com.google.protobuf.ByteString

import java.util.regex.Pattern
import scala.concurrent.{ExecutionContext, Future}

final class PackageManagementServiceIT extends LedgerTestSuite {
  private[this] val testPackageResourcePath = PackageManagementTestDar.path

  private def loadTestPackage()(implicit ec: ExecutionContext): Future[ByteString] = {
    val testPackage = Future {
      val in = getClass.getClassLoader.getResourceAsStream(testPackageResourcePath)
      assert(in != null, s"Unable to load test package resource at '$testPackageResourcePath'")
      in
    }
    val bytes = testPackage.map(ByteString.readFrom)
    bytes.onComplete(_ => testPackage.map(_.close()))
    bytes
  }

  test(
    "PMEmptyUpload",
    "An attempt at uploading an empty payload should fail",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      failure <- ledger
        .uploadDarFileAndVetOnConnectedSynchronizers(ByteString.EMPTY)
        .mustFail("uploading an empty package")
    } yield {
      assertGrpcErrorRegex(
        failure,
        PackageServiceErrors.Reading.InvalidDar,
        Some(Pattern.compile("Invalid DAR: package-upload|Dar file is corrupt")),
      )
    }
  })

  test(
    "PMDuplicateSubmissionId",
    "Duplicate submission ids are accepted when package uploaded twice",
    allocate(NoParties, NoParties),
  )(implicit ec => { case Participants(Participant(alpha, Seq()), Participant(beta, Seq())) =>
    // Multiple package updates should always succeed. Participant adds extra entropy to the
    // submission id to ensure client does not inadvertently cause problems by poor selection
    // of submission ids.
    for {
      testPackage <- loadTestPackage()
      _ <- alpha.uploadDarFileAndVetOnConnectedSynchronizers(testPackage)
      _ <- beta.uploadDarFileAndVetOnConnectedSynchronizers(testPackage)
    } yield ()
  })

  test(
    "PMLoad",
    "Concurrent uploads of the same package should be idempotent and result in the package being available for use",
    allocate(SingleParty),
    limitation = TestConstraints.GrpcOnly(
      "PackageManagementService listKnownPackages is not available in JSON"
    ),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      testPackage <- loadTestPackage()
      _ <- Future.sequence(
        Vector.fill(8)(ledger.uploadDarFileAndVetOnConnectedSynchronizers(testPackage))
      )
      knownPackages <- ledger.listKnownPackages()
      contract <- ledger.create(party, new PackageManagementTestTemplate(party))(
        PackageManagementTestTemplate.COMPANION
      )
      acsBefore <- ledger.activeContracts(Some(Seq(party)))
      _ <- ledger.exercise(party, contract.exerciseTestChoice())
      acsAfter <- ledger.activeContracts(Some(Seq(party)))
    } yield {
      val duplicatePackageIds =
        knownPackages.groupBy(_.packageId).view.mapValues(_.size).filter(_._2 > 1).toMap
      assert(
        duplicatePackageIds.isEmpty,
        s"There are duplicate package identifiers: ${duplicatePackageIds
            .map { case (name, count) => s"$name ($count)" }
            .mkString(", ")}",
      )
      assert(
        acsBefore.size == 1,
        "After the contract has been created there should be one active contract but there's none",
      )
      assert(
        acsAfter.isEmpty,
        s"There should be no active package after the contract has been consumed: ${acsAfter.map(_.contractId).mkString(", ")}",
      )
    }
  })
}
