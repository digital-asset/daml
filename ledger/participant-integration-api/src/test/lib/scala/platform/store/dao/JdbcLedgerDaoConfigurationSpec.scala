// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.platform.store.appendonlydao._
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

trait JdbcLedgerDaoConfigurationSpec {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (configuration)"

  it should "be able to persist and load configuration" in {
    val offset = nextOffset()
    val offsetString = offset.toLong
    for {
      startingOffset <- ledgerDao.lookupLedgerEnd()
      startingConfig <- ledgerDao.lookupLedgerConfiguration()

      response <- storeConfigurationEntry(
        offset,
        s"submission-$offsetString",
        defaultConfig,
      )
      optStoredConfig <- ledgerDao.lookupLedgerConfiguration()
      endingOffset <- ledgerDao.lookupLedgerEnd()
    } yield {
      response shouldEqual PersistenceResponse.Ok
      startingConfig shouldEqual None
      optStoredConfig.map(_._2) shouldEqual Some(defaultConfig)
      endingOffset.lastOffset should be > startingOffset.lastOffset
    }
  }
}
