// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.io.File
import java.nio.file.Files
import java.util.zip.ZipFile

import com.digitalasset.daml.lf.archive.{DarReader, Decode}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml_lf.DamlLf.Archive

import scala.util.{Random, Try}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  IsStatusException,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.digitalasset.ledger.api.v1.commands.CreateCommand
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField}
import com.digitalasset.ledger.client.services.admin.PackageManagementClient
import com.digitalasset.platform.apitesting.LedgerContextExtensions._
import com.digitalasset.platform.apitesting.MultiLedgerFixture
import com.digitalasset.platform.participant.util.ValueConversions._
import io.grpc.Status
import com.google.protobuf.ByteString
import org.scalatest.{AsyncFreeSpec, Matchers}
import org.scalatest.Inspectors._
import org.scalatest.concurrent.AsyncTimeLimitedTests
import scalaz.syntax.traverse._
import scalaz.std.either._
import scalaz.std.list._

import scala.concurrent.Future

class PackageManagementServiceIT
    extends AsyncFreeSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll
    with AsyncTimeLimitedTests
    with Matchers {

  private val runSuffix = "-" + Random.alphanumeric.take(10).mkString
  private val partyNameMangler =
    (partyText: String) => partyText + runSuffix + Random.alphanumeric.take(10).mkString
  private val commandIdMangler =
    (testName: String, nodeId: String) => s"ledger-api-test-tool-$testName-$nodeId-$runSuffix"

  override protected def config: Config = Config.default.copy(darFiles = Nil)

  private def packageManagementService(stub: PackageManagementService): PackageManagementClient =
    new PackageManagementClient(stub)

  private case class LoadedPackage(size: Long, archive: Archive, pkg: Ast.Package)

  private def loadTestDar: (Array[Byte], List[LoadedPackage], String) = {
    val file = new File("ledger/sandbox/Test.dar")

    val testDarBytes = Files.readAllBytes(file.toPath)

    val testPackages = DarReader {
      case (archiveSize, x) => Try(Archive.parseFrom(x)).map(ar => (archiveSize, ar))
    }.readArchive(new ZipFile(file))
      .fold(t => Left(s"Failed to parse DAR from $file: $t"), dar => Right(dar.all))
      .flatMap {
        _ traverseU {
          case (archiveSize, archive) =>
            Try(LoadedPackage(archiveSize, archive, Decode.decodeArchive(archive)._2)).toEither.left
              .map(err => s"Could not parse archive $archive.getHash: $err")
        }
      }
      .fold[List[LoadedPackage]](err => fail(err), scala.Predef.identity)

    // Guesses the package ID of the test package.
    // Note: the test DAR file contains 3 packages: the test package, stdlib, and daml-prim.
    // The test package should be by far the smallest one, so we just sort the packages by size
    // to avoid having to parse and inspect package details.
    val testPackageId = testPackages
      .sortBy(_.size)
      .headOption
      .getOrElse(fail("List of packages is empty"))
      .archive
      .getHash

    (testDarBytes, testPackages, testPackageId)
  }

  private val (testDarBytes, testPackages, testPackageId) = loadTestDar

  "should accept packages" in allFixtures { ctx =>
    val client = packageManagementService(ctx.packageManagementService)

    // Note: this may be a long running ledger, and the test package may have been uploaded before.
    // Do not make any assertions on the initial state of the ledger.
    for {
      _ <- client.uploadDarFile(ByteString.copyFrom(testDarBytes))
      finalPackages <- client.listKnownPackages()
    } yield {
      forAll(testPackages) { p =>
        finalPackages.map(_.packageId).contains(p.archive.getHash) shouldBe true
      }
      forAll(finalPackages) { p =>
        p.packageSize > 0 shouldBe true
      }
    }
  }

  "should accept duplicate packages" in allFixtures { ctx =>
    val client = packageManagementService(ctx.packageManagementService)
    val N = 8

    // Package upload is idempotent, submitting duplicate packages should succeed.
    // This test *concurrently* uploads the same package N times.
    for {
      _ <- Future.traverse(1 to N)(i => client.uploadDarFile(ByteString.copyFrom(testDarBytes)))
      finalPackages <- client.listKnownPackages()
    } yield {
      forAll(testPackages) { p =>
        finalPackages.map(_.packageId).contains(p.archive.getHash) shouldBe true
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
    val party = partyNameMangler("operator")
    val createArg = Record(fields = List(RecordField("operator", party.asParty)))
    def createCmd =
      CreateCommand(Some(Identifier(testPackageId, "", "Test", "Dummy")), Some(createArg)).wrap
    val filter = TransactionFilter(Map(party -> Filters.defaultInstance))
    val client = packageManagementService(ctx.packageManagementService)

    for {
      _ <- client.uploadDarFile(ByteString.copyFrom(testDarBytes))
      createTx <- ctx.testingHelpers.submitAndListenForSingleResultOfCommand(
        ctx.testingHelpers
          .submitRequestWithId(commandIdMangler("PackageManagementServiceIT_commands", "create"))
          .update(
            _.commands.commands := List(createCmd),
            _.commands.party := party
          ),
        filter
      )
      createdEv = ctx.testingHelpers.getHead(ctx.testingHelpers.createdEventsIn(createTx))
    } yield {
      createdEv.templateId.map(_.packageId) shouldBe Some(testPackageId)
    }
  }
}
