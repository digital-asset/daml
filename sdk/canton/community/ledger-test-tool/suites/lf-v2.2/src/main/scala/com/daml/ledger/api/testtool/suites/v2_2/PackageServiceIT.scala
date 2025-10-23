// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors

import scala.concurrent.Future

final class PackageServiceIT extends LedgerTestSuite {

  /** A package ID that is guaranteed to not be uploaded */
  private[this] val unknownPackageId = " "

  test("PackagesList", "Listing packages should return a result", allocate(NoParties))(
    implicit ec => { case Participants(Participant(ledger, Seq())) =>
      for {
        knownPackages <- ledger.listPackages()
      } yield assert(
        knownPackages.size >= 3,
        s"List of packages was expected to contain at least 3 packages, got ${knownPackages.size} instead.",
      )
    }
  )

  test(
    "PackagesGetKnown",
    "Getting package content should return a valid result",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      somePackageId <- ledger
        .listPackages()
        .map(_.headOption.getOrElse(fail("No package found")))
      somePackage <- ledger.getPackage(somePackageId)
    } yield {
      assert(somePackage.hash.length > 0, s"Package $somePackageId has an empty hash.")
      assert(
        somePackage.hash == somePackageId,
        s"Package $somePackageId has hash ${somePackage.hash}, expected hash to be equal to the package ID.",
      )
      assert(somePackage.archivePayload.size() >= 0, s"Package $somePackageId has zero size.")
    }
  })

  test(
    "PackagesGetUnknown",
    "Getting package content for an unknown package should fail",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      failure <- ledger
        .getPackage(unknownPackageId)
        .mustFail("getting the contents of an unknown package")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Package,
        None,
      )
    }
  })

  test(
    "PackagesStatusKnown",
    "Getting package status should return a valid result",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      packageIds <- ledger
        .listPackages()
        .map(packages => packages.headOption.fold(fail("No package found"))(_ => packages))
      statuses <- Future.traverse(packageIds)(ledger.getPackageStatus)
    } yield {
      statuses
        .collectFirst(_.isPackageStatusRegistered)
        .getOrElse(fail(s"No registered package found among: ${packageIds.toString}"))
    }
  })

  test(
    "PackagesStatusUnknown",
    "Getting package status for an unknown package should fail",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      status <- ledger.getPackageStatus(unknownPackageId)
    } yield {
      assert(status.isPackageStatusUnspecified, s"Package $unknownPackageId is not unknown.")
    }
  })
}
