// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.util.UUID

import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, IsStatusException, SuiteResourceManagementAroundAll}
import com.digitalasset.ledger.api.v1.package_service.PackageStatus
import com.digitalasset.ledger.client.services.pkg.PackageClient
import com.digitalasset.platform.apitesting.{LedgerContext, MultiLedgerFixture}
import io.grpc.Status
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import org.scalatest.{AsyncWordSpec, Matchers, OptionValues}

import scalaz.syntax.tag._

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

  private def client(ctx: LedgerContext): PackageClient = {
    new PackageClient(ctx.ledgerId, ctx.packageService)
  }

  private def getARegisteredPackageId(ctx: LedgerContext) =
    client(ctx).listPackages().map(_.packageIds.headOption.value)

  "Package Service" when {

    "asked for the list of registered packages" should {

      "return it" in allFixtures { context =>
        client(context).listPackages() map {
          _.packageIds.size shouldEqual 3 // package, stdlib, daml-prim
        }
      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        new PackageClient(domain.LedgerId(s"not-${context.ledgerId.unwrap}"), context.packageService)
          .listPackages()
          .failed map {
          IsStatusException(Status.NOT_FOUND)(_)
        }

      }
    }

    "asked to get a package" should {

      "return it if it's registered" in allFixtures { context =>
        getARegisteredPackageId(context)
          .flatMap(client(context).getPackage(_)) map {
          _.archivePayload.size() should be > 0
        }

      }

      "return a NOT_FOUND error if it's not registered" in allFixtures { context =>
        client(context).getPackage(UUID.randomUUID().toString).failed map {
          IsStatusException(Status.NOT_FOUND)(_)
        }
      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        getARegisteredPackageId(context)
          .flatMap(
            new PackageClient(
              domain.LedgerId(s"not-${context.ledgerId.unwrap}"),
              context.packageService)
              .getPackage(_)
              .failed) map {
          IsStatusException(Status.NOT_FOUND)(_)
        }
      }
    }

    "asked to check a package's status" should {

      "return true if it's registered" in allFixtures { context =>
        getARegisteredPackageId(context)
          .flatMap(client(context).getPackageStatus(_)) map {
          _.packageStatus shouldEqual PackageStatus.REGISTERED
        }
      }

      "return false if it's not registered" in allFixtures { context =>
        client(context).getPackageStatus(UUID.randomUUID().toString) map {
          _.packageStatus shouldEqual PackageStatus.UNKNOWN
        }

      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        getARegisteredPackageId(context)
          .flatMap(
            new PackageClient(
              domain.LedgerId(s"not-${context.ledgerId.unwrap}"),
              context.packageService)
              .getPackageStatus(_)
              .failed) map {
          IsStatusException(Status.NOT_FOUND)(_)
        }
      }
    }
  }

  override protected def config: Config = Config.default
}
