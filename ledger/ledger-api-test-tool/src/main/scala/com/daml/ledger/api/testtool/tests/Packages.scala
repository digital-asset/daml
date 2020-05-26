// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import io.grpc.Status

final class Packages(session: LedgerSession) extends LedgerTestSuite(session) {

  /** A package ID that is guaranteed to not be uploaded */
  private[this] val unknownPackageId = " "

  test("PackagesList", "Listing packages should return a result", allocate(NoParties)) {
    case Participants(Participant(ledger)) =>
      for {
        knownPackages <- ledger.listPackages()
      } yield
        assert(
          knownPackages.size >= 3,
          s"List of packages was expected to contain at least 3 packages, got ${knownPackages.size} instead.",
        )
  }

  test("PackagesGet", "Getting package content should return a valid result", allocate(NoParties)) {
    case Participants(Participant(ledger)) =>
      for {
        somePackageId <- ledger.listPackages().map(_.headOption.getOrElse(fail("No package found")))
        somePackage <- ledger.getPackage(somePackageId)
      } yield {
        assert(somePackage.hash.length > 0, s"Package $somePackageId has an empty hash.")
        assert(
          somePackage.hash == somePackageId,
          s"Package $somePackageId has hash ${somePackage.hash}, expected hash to be equal to the package ID.",
        )
        assert(somePackage.archivePayload.size() >= 0, s"Package $somePackageId has zero size.")
      }
  }

  test(
    "PackagesGetUnknown",
    "Getting package content for an unknown package should fail",
    allocate(NoParties),
  ) {
    case Participants(Participant(ledger)) =>
      for {
        failure <- ledger.getPackage(unknownPackageId).failed
      } yield {
        assertGrpcError(failure, Status.Code.NOT_FOUND, "")
      }
  }

  test("PackagesStatus", "Getting package status should return a valid result", allocate(NoParties)) {
    case Participants(Participant(ledger)) =>
      for {
        somePackageId <- ledger.listPackages().map(_.headOption.getOrElse(fail("No package found")))
        status <- ledger.getPackageStatus(somePackageId)
      } yield {
        assert(status.isRegistered, s"Package $somePackageId is not registered.")
      }
  }

  test(
    "PackagesStatusUnknown",
    "Getting package status for an unknown package should fail",
    allocate(NoParties),
  ) {
    case Participants(Participant(ledger)) =>
      for {
        status <- ledger.getPackageStatus(unknownPackageId)
      } yield {
        assert(status.isUnknown, s"Package $unknownPackageId is not unknown.")
      }
  }
}
