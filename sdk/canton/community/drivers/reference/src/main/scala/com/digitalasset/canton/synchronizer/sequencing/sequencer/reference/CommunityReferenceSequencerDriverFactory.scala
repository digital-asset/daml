// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.reference

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.{DbConfig, ProcessingTimeout, StorageConfig}
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{CommunityStorageSetup, Storage}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import monocle.macros.syntax.lens.*

import scala.concurrent.ExecutionContext

final class CommunityReferenceSequencerDriverFactory extends BaseReferenceSequencerDriverFactory {

  override def name: String = "community-reference"

  override protected def createStorage(
      config: ReferenceSequencerDriver.Config[StorageConfig],
      clock: Clock,
      processingTimeout: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
      closeContext: CloseContext,
      metricsContext: MetricsContext,
  ): Storage =
    CommunityStorageSetup.tryCreateAndMigrateStorage(
      config.storage,
      config.logQueryCost,
      clock,
      processingTimeout,
      loggerFactory,
      setMigrationsPath,
    )

  def setMigrationsPath(config: StorageConfig): StorageConfig =
    config match {
      case h2: DbConfig.H2 =>
        h2.focus(_.parameters.migrationsPaths)
          .replace(Seq("classpath:db/migration/canton/h2/dev/reference/"))
      case pg: DbConfig.Postgres =>
        pg.focus(_.parameters.migrationsPaths)
          .replace(Seq("classpath:db/migration/canton/postgres/dev/reference/"))
      case x => x
    }
}
