// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client

import com.daml.integrationtest.CantonFixture
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import com.daml.ledger.api.domain
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}

import scala.concurrent.{Future}
import com.google.protobuf.ByteString
import com.daml.bazeltools.BazelRunfiles._
import java.io.File
import java.io.FileInputStream
import com.daml.lf.language.Ast
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.archive.{DarDecoder, Dar}
import org.scalatest.Suite

final class UpgradesIT extends AsyncWordSpec with Matchers with Inside with CantonFixture {
  self: Suite =>
  override lazy val devMode = true;
  override val cantonFixtureDebugMode = true;

  private def loadTestPackageBS(version: Int): Future[ByteString] = {
    val path = s"test-common/upgrades-example-v$version.dar"
    val testPackage = Future {
      val in = new FileInputStream(new File(rlocation(path)))
      assert(in != null, s"Unable to load test package resource '$path'")
      in
    }
    val bytes = testPackage.map(ByteString.readFrom)
    bytes.onComplete(_ => testPackage.map(_.close()))
    bytes
  }

  private def loadTestPackageDar(version: Int): Future[Dar[(PackageId, Ast.Package)]] = {
    val path = s"test-common/upgrades-example-v$version.dar"
    Future {
      val in = DarDecoder.assertReadArchiveFromFile(new File(rlocation(path)))
      assert(in != null, s"Unable to load test package resource '$path'")
      in
    }
  }

  private val LedgerId = domain.LedgerId(config.ledgerIds.head)

  lazy val channel = config.channel(ports.head)

  private val ClientConfiguration = LedgerClientConfiguration(
    applicationId = applicationId.get,
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
    token = None,
  )

  "this test" should {
    "run a simple check" in {
      1 shouldEqual 1

      for {
        client <- LedgerClient(channel, ClientConfiguration)
        testPackageV1BS <- loadTestPackageBS(1)
        testPackageV2BS <- loadTestPackageBS(2)
        _ <- client.packageManagementClient.uploadDarFile(testPackageV1BS)
        _ <- client.packageManagementClient.uploadDarFile(testPackageV2BS)
        _ <- loadTestPackageDar(1)
        _ <- loadTestPackageDar(2)
        // _ <- Future { Thread.sleep(10 * 1000) }
        // _ <- client.packageManagementClient.listKnownPackages()
      } yield {
        client.ledgerId shouldBe LedgerId
        0 shouldBe 1
      }
    }
  }
}
