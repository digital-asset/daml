// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.util.UUID

import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  IsStatusException,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.digitalasset.ledger.api.v1.package_service.PackageStatus
import com.digitalasset.ledger.client.services.pkg.PackageClient
import com.digitalasset.platform.apitesting.MultiLedgerFixture
import io.grpc.Status
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import org.scalatest.{AsyncWordSpec, Matchers, OptionValues}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class PackageServiceIT
    extends AsyncWordSpec
    with MultiLedgerFixture
    with AkkaBeforeAndAfterAll
    with SuiteResourceManagementAroundAll
    with AsyncTimeLimitedTests
    with Matchers
    with OptionValues {

  override def timeLimit: Span = 5.seconds

  private def client(stub: PackageService): PackageClient = {
    client(stub, config.getLedgerId)
  }

  private def client(stub: PackageService, ledgerId: String): PackageClient = {
    new PackageClient(ledgerId, stub)
  }

  private def getARegisteredPackageId(stub: PackageService) =
    client(stub).listPackages().map(_.packageIds.headOption.value)

  "Package Service" when {

    "asked for the list of registered packages" should {

      "return it" in allFixtures { context =>
        client(context.packageService).listPackages() map {
          _.packageIds.size shouldEqual 2
        }
      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        client(context.packageService, "not " + config.getLedgerId).listPackages().failed map {
          IsStatusException(Status.NOT_FOUND)(_)
        }

      }
    }

    "asked to get a package" should {

      "return it if it's registered" in allFixtures { context =>
        getARegisteredPackageId(context.packageService)
          .flatMap(client(context.packageService).getPackage(_)) map {
          _.archivePayload.size() should be > 0
        }

      }

      "return a NOT_FOUND error if it's not registered" in allFixtures { context =>
        client(context.packageService).getPackage(UUID.randomUUID().toString).failed map {
          IsStatusException(Status.NOT_FOUND)(_)
        }
      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        getARegisteredPackageId(context.packageService)
          .flatMap(client(context.packageService, "not " + config.getLedgerId).getPackage(_).failed) map {
          IsStatusException(Status.NOT_FOUND)(_)
        }
      }
    }

    "asked to check a package's status" should {

      "return true if it's registered" in allFixtures { context =>
        getARegisteredPackageId(context.packageService)
          .flatMap(client(context.packageService).getPackageStatus(_)) map {
          _.packageStatus shouldEqual PackageStatus.REGISTERED
        }
      }

      "return false if it's not registered" in allFixtures { context =>
        client(context.packageService).getPackageStatus(UUID.randomUUID().toString) map {
          _.packageStatus shouldEqual PackageStatus.UNKNOWN
        }

      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        getARegisteredPackageId(context.packageService)
          .flatMap(
            client(context.packageService, "not " + config.getLedgerId)
              .getPackageStatus(_)
              .failed) map {
          IsStatusException(Status.NOT_FOUND)(_)
        }
      }
    }
  }

  override protected def config: Config = Config.default
}
