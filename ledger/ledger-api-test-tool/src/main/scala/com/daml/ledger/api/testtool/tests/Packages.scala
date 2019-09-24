// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import io.grpc.Status

final class Packages(session: LedgerSession) extends LedgerTestSuite(session) {

  /** A package ID that is guaranteed to not be uploaded */
  private[this] val unknownPackageId = " "

  private[this] val listPackages =
    LedgerTest("PackagesList", "Listing packages should return a result") { context =>
      for {
        ledger <- context.participant()
        knownPackages <- ledger.listPackages()
      } yield
        assert(
          knownPackages.size >= 3,
          s"List of packages was expected to contain at least 3 packages, got ${knownPackages.size} instead.")
    }

  private[this] val getPackage =
    LedgerTest("PackagesGet", "Getting package content should return a valid result") { context =>
      for {
        ledger <- context.participant()
        somePackageId <- ledger.listPackages().map(_.headOption.getOrElse(fail("No package found")))
        somePackage <- ledger.getPackage(somePackageId)
      } yield {
        assert(somePackage.hash.length > 0, s"Package $somePackageId has an empty hash.")
        assert(somePackage.archivePayload.size() >= 0, s"Package $somePackageId has zero size.")
      }
    }

  private[this] val getUnknownPackage =
    LedgerTest("PackagesGetUnknown", "Getting package content for an unknown package should fail") {
      context =>
        for {
          ledger <- context.participant()
          failure <- ledger.getPackage(unknownPackageId).failed
        } yield {
          assertGrpcError(failure, Status.Code.NOT_FOUND, "")
        }
    }

  private[this] val getPackageStatus =
    LedgerTest("PackagesStatus", "Getting package status should return a valid result") { context =>
      for {
        ledger <- context.participant()
        somePackageId <- ledger.listPackages().map(_.headOption.getOrElse(fail("No package found")))
        status <- ledger.getPackageStatus(somePackageId)
      } yield {
        assert(status.isRegistered, s"Package $somePackageId is not registered.")
      }
    }

  private[this] val getUnknownPackageStatus =
    LedgerTest("PackagesStatusUnknown", "Getting package status for an unknown package should fail") { context =>
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
