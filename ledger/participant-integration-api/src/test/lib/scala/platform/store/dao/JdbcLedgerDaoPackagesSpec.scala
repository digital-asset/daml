// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.ledger.participant.state.v1.Offset
import org.scalatest.{AsyncFlatSpec, Matchers}

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
          .take(1))
      secondUploadResult <- storePackageEntry(
        offset2,
        packages.map(a => a._1 -> a._2.copy(sourceDescription = Some(secondDescription))))
      loadedPackages <- ledgerDao.listLfPackages
    } yield {
      firstUploadResult shouldBe PersistenceResponse.Ok
      secondUploadResult shouldBe PersistenceResponse.Ok
      // Note that the order here isn’t fixed.
      loadedPackages.values.flatMap(_.sourceDescription.toList) should contain theSameElementsAs
        Seq(firstDescription) ++ Seq.fill(packages.length - 1)(secondDescription)
    }
  }

  private def storePackageEntry(
      offset: Offset,
      packageList: List[(DamlLf.Archive, PackageDetails)]) = {
    ledgerDao
      .storePackageEntry(previousOffset.get(), offset, packageList, None)
      .map { r =>
        previousOffset.set(Some(offset))
        r
      }
  }
}
