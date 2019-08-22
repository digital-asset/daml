// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.packagemanagementtest.PackageManagementTest.PackageManagementTestTemplate
import com.digitalasset.ledger.packagemanagementtest.PackageManagementTest.PackageManagementTestTemplate._
import com.google.protobuf.ByteString
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

final class PackageManagement(session: LedgerSession) extends LedgerTestSuite(session) {

  private[this] val testPackageResourcePath =
    "/ledger/ledger-api-test-tool/PackageManagementTest.dar"

  private def loadTestPackage()(implicit ec: ExecutionContext): Future[ByteString] = {
    val testPackage = Future {
      val in = getClass.getResourceAsStream(testPackageResourcePath)
      assert(in != null, s"Unable to load test package resource at '$testPackageResourcePath'")
      in
    }
    val bytes = testPackage.map(ByteString.readFrom)
    bytes.onComplete(_ => testPackage.map(_.close()))
    bytes
  }

  private[this] val emptyUpload =
    LedgerTest(
      "PackageManagementEmptyUpload",
      "An attempt at uploading an empty payload should fail"
    ) {
      _.uploadDarFile(ByteString.EMPTY).failed.map {
        assertGrpcError(
          _,
          Status.Code.INVALID_ARGUMENT,
          "Invalid argument: Invalid DAR: package-upload")
      }
    }

  private[this] val concurrentUploadAndUsage =
    LedgerTest(
      "PackageManagementLoad",
      "Concurrent uploads of the same package should be idempotent and result in the package being available for use") {
      ledger =>
        for {
          testPackage <- loadTestPackage()
          _ <- Future.sequence(Vector.fill(8)(ledger.uploadDarFile(testPackage)))
          knownPackages <- ledger.listKnownPackages()
          owner <- ledger.allocateParty()
          contract <- ledger.create(owner, new PackageManagementTestTemplate(owner))
          acsBefore <- ledger.activeContracts(owner)
          _ <- ledger.exercise(owner, contract.exerciseTestChoice)
          acsAfter <- ledger.activeContracts(owner)
        } yield {
          val duplicatePackageIds =
            knownPackages.groupBy(_.packageId).mapValues(_.size).filter(_._2 > 1)
          assert(
            duplicatePackageIds.isEmpty,
            s"There are duplicate package identifiers: ${duplicatePackageIds map {
              case (name, count) => s"$name ($count)"
            } mkString (", ")}")
          assert(
            acsBefore.size == 1,
            s"After the contract has been created there should be one active contract but there's none")
          assert(
            acsAfter.isEmpty,
            s"There should be no active package after the contract has been consumed: ${acsAfter.map(_.contractId).mkString(", ")}")
        }
    }

  override val tests: Vector[LedgerTest] = Vector(concurrentUploadAndUsage, emptyUpload)
}
