// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import akka.stream.scaladsl.Sink
import com.daml.platform.store.entries.ConfigurationEntry
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

// TODO append-only: remove this class once the mutating schema is removed
trait JdbcLedgerDaoConfigurationMutatingSpec {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (configuration-mutating)"

  it should "refuse to persist invalid configuration entry" in {
    val startExclusive = nextOffset()
    val offset0 = nextOffset()
    val offsetString0 = offset0.toLong
    for {
      config <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2).getOrElse(defaultConfig))

      // Store a new configuration with a known submission id
      submissionId = s"refuse-config-$offsetString0"
      resp0 <- storeConfigurationEntry(
        offset0,
        submissionId,
        config.copy(generation = config.generation + 1),
      )
      newConfig <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2).get)

      resp1 <- storeConfigurationEntry(
        nextOffset(),
        submissionId,
        newConfig.copy(generation = config.generation + 1),
      )

      // Submission with mismatching generation is rejected
      offset2 = nextOffset()
      offsetString2 = offset2.toLong
      resp2 <- storeConfigurationEntry(
        offset2,
        s"refuse-config-$offsetString2",
        config,
      )

      // Submission with unique submissionId and correct generation is accepted.
      offset3 = nextOffset()
      offsetString3 = offset3.toLong
      lastConfig = newConfig.copy(generation = newConfig.generation + 1)
      resp3 <- storeConfigurationEntry(offset3, s"refuse-config-$offsetString3", lastConfig)
      lastConfigActual <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2).get)

      entries <- ledgerDao.getConfigurationEntries(startExclusive, offset3).runWith(Sink.seq)
    } yield {
      resp0 shouldEqual PersistenceResponse.Ok
      resp1 shouldEqual PersistenceResponse.Duplicate
      resp2 shouldEqual PersistenceResponse.Ok
      resp3 shouldEqual PersistenceResponse.Ok
      lastConfig shouldEqual lastConfigActual
      entries.toList shouldEqual List(
        offset0 -> ConfigurationEntry.Accepted(s"refuse-config-$offsetString0", newConfig),
        /* offset1 is duplicate */
        offset2 -> ConfigurationEntry.Rejected(
          s"refuse-config-${offset2.toLong}",
          "Generation mismatch: expected=2, actual=0",
          config,
        ),
        offset3 -> ConfigurationEntry.Accepted(s"refuse-config-$offsetString3", lastConfig),
      )
    }
  }
}
