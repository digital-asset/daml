// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.data.Time.Timestamp
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.index.PackageDetails
import com.digitalasset.canton.platform.store.dao.*
import com.digitalasset.canton.platform.store.entries.PackageLedgerEntry
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

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
      PackageLedgerEntry.PackageUploadAccepted(UUID.randomUUID().toString, Timestamp.Epoch)
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
        Timestamp.Epoch,
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

  private def storePackageEntry(
      offset: Offset,
      packageList: List[(DamlLf.Archive, PackageDetails)],
      optEntry: Option[PackageLedgerEntry] = None,
  ) =
    ledgerDao
      .storePackageEntry(offset, packageList, optEntry)
}
