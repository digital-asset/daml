package com.digitalasset.platform.tests.integration.ledger.api

import com.digitalasset.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, SuiteResourceManagementAroundAll}
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.digitalasset.ledger.client.services.admin.PackageManagementClient
import com.digitalasset.platform.apitesting.{MultiLedgerFixture, TestTemplateIds}
import org.scalatest.{AsyncFreeSpec, AsyncWordSpec, Matchers}
import org.scalatest.concurrent.AsyncTimeLimitedTests

class PackageManagementServiceIT
    extends AsyncFreeSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll
    with AsyncTimeLimitedTests
    with Matchers
    with TestTemplateIds {
  override protected def config: Config = Config.default.copy(darFiles = Nil)

  private def packageManagementService(stub: PackageManagementService): PackageManagementClient =
    new PackageManagementClient(stub)

  "should accept packages" in allFixtures { ctx =>
    val client = packageManagementService(ctx.packageManagementService)
    for {
      pkgs <- client.listKnownPackages()
    } yield {
      pkgs.isEmpty shouldBe true
    }
  }

}
