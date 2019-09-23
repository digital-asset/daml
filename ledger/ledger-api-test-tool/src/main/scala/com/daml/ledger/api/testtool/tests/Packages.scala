// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import io.grpc.Status

final class Packages(session: LedgerSession) extends LedgerTestSuite(session) {

  /** A package ID that is guaranteed to not be uploaded */
  val unknownPackageId = " "

  val listPackages =
    LedgerTest("Packages", "Listing packages should return a result") { context =>
      for {
        ledger <- context.participant()
        knownPackages <- ledger.listPackages()
      } yield
        assert(
          knownPackages.size >= 3,
          s"List of packages was expected to contain at least 3 packages, got ${knownPackages.size} instead.")
    }

  val getPackage =
    LedgerTest("Packages", "Getting package content should return a valid result") { context =>
      for {
        ledger <- context.participant()
        somePackageId <- ledger.listPackages().map(_.headOption.getOrElse(fail("No package found")))
        somePackage <- ledger.getPackage(somePackageId)
      } yield {
        assert(somePackage.hash.length > 0, s"Package $somePackageId has an empty hash.")
        assert(somePackage.archivePayload.size() >= 0, s"Package $somePackageId has zero size.")
      }
    }

  val getUnknownPackage =
    LedgerTest("Packages", "Getting package content for an unknown package should fail") {
      context =>
        for {
          ledger <- context.participant()
          failure <- ledger.getPackage(unknownPackageId).failed
        } yield {
          assertGrpcError(failure, Status.Code.NOT_FOUND, "")
        }
    }

  val getPackageStatus =
    LedgerTest("Packages", "Getting package status should return a valid result") { context =>
      for {
        ledger <- context.participant()
        somePackageId <- ledger.listPackages().map(_.headOption.getOrElse(fail("No package found")))
        status <- ledger.getPackageStatus(somePackageId)
      } yield {
        assert(status.isRegistered, s"Package $somePackageId is not registered.")
      }
    }

  val getUnknownPackageStatus =
    LedgerTest("Packages", "Getting package status for an unknown package should fail") { context =>
      for {
        ledger <- context.participant()
        status <- ledger.getPackageStatus(unknownPackageId)
      } yield {
        assert(status.isUnknown, s"Package $unknownPackageId is not unknown.")
      }
    }

  override val tests: Vector[LedgerTest] = Vector(
    listPackages,
    getPackage,
    getUnknownPackage,
    getPackageStatus,
    getUnknownPackageStatus
  )

}
