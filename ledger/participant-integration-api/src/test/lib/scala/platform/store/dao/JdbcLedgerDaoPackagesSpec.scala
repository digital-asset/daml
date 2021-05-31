// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant
import java.util.UUID

import akka.stream.scaladsl.Sink
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.ledger.participant.state.v1.Offset
import com.daml.platform.indexer.IncrementalOffsetStep
import com.daml.platform.store.dao.ParametersTable.LedgerEndUpdateError
import com.daml.platform.store.entries.PackageLedgerEntry

private[dao] trait JdbcLedgerDaoPackagesSpec {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (packages)"

  it should "upload packages in an idempotent fashion, maintaining existing descriptions" in {
    val firstDescription = "first description"
    val secondDescription = "second description"
    val offset1 = nextOffset()
    val offset2 = nextOffset()
    for {
      firstUploadResult <- storePackageEntry(
        offset1,
        packages
          .map(a => a._1 -> a._2.copy(sourceDescription = Some(firstDescription)))
          .take(1),
      )
      secondUploadResult <- storePackageEntry(
        offset2,
        packages.map(a => a._1 -> a._2.copy(sourceDescription = Some(secondDescription))),
      )
      loadedPackages <- ledgerDao.listLfPackages()
    } yield {
      firstUploadResult shouldBe PersistenceResponse.Ok
      secondUploadResult shouldBe PersistenceResponse.Ok
      // Note that the order here isn’t fixed.
      loadedPackages.values.flatMap(_.sourceDescription.toList) should contain theSameElementsAs
        Seq(firstDescription) ++ Seq.fill(packages.length - 1)(secondDescription)
    }
  }

  it should "upload packages with accept and reject entries" in {
    val firstDescription = "first description"
    val secondDescription = "second description"
    val offset1 = nextOffset()
    val offset2 = nextOffset()
    val offset3 = nextOffset()
    val accepted1 =
      PackageLedgerEntry.PackageUploadAccepted(UUID.randomUUID().toString, Instant.EPOCH)
    for {
      uploadAcceptedResult <- storePackageEntry(
        offset = offset2,
        packageList = packages
          .map(a => a._1 -> a._2.copy(sourceDescription = Some(firstDescription)))
          .take(1),
        optEntry = Option(accepted1),
      )
      rejected1 = PackageLedgerEntry.PackageUploadRejected(
        UUID.randomUUID().toString,
        Instant.EPOCH,
        "some rejection reason",
      )
      uploadRejectedResult <- storePackageEntry(
        offset = offset3,
        packageList =
          packages.map(a => a._1 -> a._2.copy(sourceDescription = Some(secondDescription))),
        optEntry = Option(rejected1),
      )
      loadedPackages <- ledgerDao.listLfPackages()
      // returns a Source so need to run with a dummy Sink to get value
      packageEntries <- ledgerDao.getPackageEntries(offset1, offset3).take(2).runWith(Sink.seq)
    } yield {
      uploadAcceptedResult shouldBe PersistenceResponse.Ok
      uploadRejectedResult shouldBe PersistenceResponse.Ok
      // Note that the order here isn’t fixed.
      loadedPackages.values.flatMap(_.sourceDescription.toList) should contain theSameElementsAs
        Seq(firstDescription) ++ Seq.fill(packages.length - 1)(secondDescription)

      // ensure we can retrieve package accept/reject entries
      assert(packageEntries == Vector((offset2, accepted1), (offset3, rejected1)))

    }
  }

  it should "fail on storing package entry with non-incremental offsets" in {
    val offset = nextOffset()
    recoverToSucceededIf[LedgerEndUpdateError](
      ledgerDao
        .storePackageEntry(IncrementalOffsetStep(offset, offset), packages, None)
    )
  }

  private def storePackageEntry(
      offset: Offset,
      packageList: List[(DamlLf.Archive, PackageDetails)],
      optEntry: Option[PackageLedgerEntry] = None,
  ) =
    ledgerDao
      .storePackageEntry(nextOffsetStep(offset), packageList, optEntry)
}
