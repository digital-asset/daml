// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.LedgerApiServerBootstrapUtils.IndexerLockIds
import com.digitalasset.canton.participant.config.*
import com.digitalasset.canton.platform.indexer.ha.HaConfig
import com.digitalasset.canton.time.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.engine.Engine

final case class LedgerApiServerBootstrapUtils(
    engine: Engine,
    clock: Clock,
    testingTimeService: TestingTimeService,
    allocateIndexerLockIds: DbConfig => Either[String, Option[IndexerLockIds]],
    loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  def createHaConfig(config: ParticipantNodeConfig)(implicit
      traceContext: TraceContext
  ): HaConfig =
    config.storage match {
      case _: DbConfig.H2 =>
        // For H2 the non-unique indexer lock ids are sufficient.
        logger.debug("Not allocating indexer lock IDs on H2 config")
        HaConfig()

      case dbConfig: DbConfig =>
        allocateIndexerLockIds(dbConfig).fold(
          err => throw new IllegalStateException(s"Failed to allocated lock IDs for indexer: $err"),
          _.fold(HaConfig()) { case IndexerLockIds(mainLockId, workerLockId) =>
            HaConfig(indexerLockId = mainLockId, indexerWorkerLockId = workerLockId)
          },
        )

      case _ =>
        logger.debug("Not allocating indexer lock IDs on non-DB config")
        HaConfig()
    }
}

object LedgerApiServerBootstrapUtils {
  final case class IndexerLockIds(mainLockId: Int, workerLockId: Int)
}
