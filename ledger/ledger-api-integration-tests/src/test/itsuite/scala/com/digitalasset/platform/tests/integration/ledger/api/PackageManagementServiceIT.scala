// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.nio.file.{Files, Paths}

import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  IsStatusException,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageDetails
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.digitalasset.ledger.api.v1.commands.CreateCommand
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField}
import com.digitalasset.ledger.client.services.admin.PackageManagementClient
import com.digitalasset.platform.apitesting.LedgerContextExtensions._
import com.digitalasset.platform.apitesting.MultiLedgerFixture
import com.digitalasset.platform.participant.util.ValueConversions._
import com.google.protobuf.ByteString
import io.grpc.Status
import org.scalatest.{AsyncFreeSpec, Matchers}
import org.scalatest.Inspectors._
import org.scalatest.concurrent.AsyncTimeLimitedTests

class PackageManagementServiceIT
    extends AsyncFreeSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll
    with AsyncTimeLimitedTests
    with Matchers {
  override protected def config: Config = Config.default.copy(darFiles = Nil)

  private def packageManagementService(stub: PackageManagementService): PackageManagementClient =
    new PackageManagementClient(stub)

  /**
    * Given a list of DAML-LF packages, guesses the package ID of the test package.
    * Note: the test DAR file contains 3 packages: the test package, stdlib, and daml-prim.
    * The test package should be by far the smallest one, so we just sort the packages by size
    * to avoid having to parse and inspect package details.
    */
  private def findTestPackageId(packages: Seq[PackageDetails]): String =
    packages
      .sortBy(_.packageSize)
      .headOption
      .getOrElse(fail("List of packages is empty"))
      .packageId

  "should accept packages" in allFixtures { ctx =>
    val darFile = Files.readAllBytes(Paths.get("ledger/sandbox/Test.dar"))
    val client = packageManagementService(ctx.packageManagementService)
    for {
      initialPackages <- client.listKnownPackages()
      _ <- client.uploadDarFile(ByteString.copyFrom(darFile))
      finalPackages <- client.listKnownPackages()
    } yield {
      initialPackages should have size 0
      finalPackages should have size 3 // package, stdlib, daml-prim
      forAll(finalPackages) { p =>
        p.packageSize > 0 shouldBe true
      }
    }
  }

  "fail with the expected status on an invalid upload" in allFixtures { ctx =>
    packageManagementService(ctx.packageManagementService)
      .uploadDarFile(ByteString.EMPTY)
      .failed map { ex =>
      IsStatusException(Status.INVALID_ARGUMENT.getCode)(ex)
    }
  }

  "should accept commands using the uploaded package" in allFixtures { ctx =>
    val darFile = Files.readAllBytes(Paths.get("ledger/sandbox/Test.dar"))
    val party = "operator"
    val createArg = Record(fields = List(RecordField("operator", party.asParty)))
    def createCmd(packageId: String) =
      CreateCommand(Some(Identifier(packageId, "", "Test", "Dummy")), Some(createArg)).wrap
    val filter = TransactionFilter(Map(party -> Filters.defaultInstance))
    val client = packageManagementService(ctx.packageManagementService)

    for {
      _ <- client.uploadDarFile(ByteString.copyFrom(darFile))
      packages <- client.listKnownPackages()
      packageId = findTestPackageId(packages)
      createTx <- ctx.testingHelpers.submitAndListenForSingleResultOfCommand(
        ctx.testingHelpers
          .submitRequestWithId("create")
          .update(
            _.commands.commands := List(createCmd(packageId)),
            _.commands.party := party
          ),
        filter
      )
      createdEv = ctx.testingHelpers.getHead(ctx.testingHelpers.createdEventsIn(createTx))
    } yield {
      createdEv.templateId.map(_.packageId) shouldBe Some(packageId)
    }
  }
}
