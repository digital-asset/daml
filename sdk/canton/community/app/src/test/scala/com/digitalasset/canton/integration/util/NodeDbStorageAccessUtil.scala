// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.resource.{CommunityStorageFactory, DbStorage}

import scala.concurrent.ExecutionContext

/** This trait provides utility methods to access the db storage of a node in tests and specific
  * utilities:
  *   - counting the number of sequenced events received by a node
  */
trait NodeDbStorageAccessUtil {
  self: BaseTest =>

  /** Returns the storage for a given local node, which is expected to be a DbStorage instance.
    * Throws an exception if the storage is not of type DbStorage. For remote nodes search usages of
    * `createStorageFor`.
    */
  protected def dbStorageFor(
      node: LocalInstanceReference
  )(implicit ec: ExecutionContext, cc: CloseContext): DbStorage =
    new CommunityStorageFactory(node.config.storage)
      .tryCreate(
        connectionPoolForParticipant = false,
        logQueryCost = None,
        clock = wallClock,
        scheduler = None,
        metrics = CommonMockMetrics.dbStorage,
        timeouts = DefaultProcessingTimeouts.testing,
        loggerFactory = loggerFactory,
      ) match {
      case dbStorage: DbStorage => dbStorage
      case _ => fail("expected be db storage")
    }

  /** `dbStorageFor` variant with lifecycle management of the storage object */
  protected def withDbStorageFor[T](
      node: LocalInstanceReference
  )(f: DbStorage => T)(implicit ec: ExecutionContext, cc: CloseContext): T = {
    val dbStorage = dbStorageFor(node)
    try f(dbStorage)
    finally dbStorage.close()
  }

  /** Counts the number of sequenced events in the sequencer client table of a local node.
    */
  protected def countSequencedEventsReceivedBy(
      node: LocalInstanceReference
  )(implicit ec: ExecutionContext, cc: CloseContext): Int =
    withDbStorageFor(node) { dbStorage =>
      import dbStorage.api.*
      dbStorage
        .query(
          sql"""select count(*) from common_sequenced_events""".as[Int],
          "count sequenced events received",
        )
        .map(_.sum)
        .futureValueUS
    }
}
