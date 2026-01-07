// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.operations

import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  IsolatedEnvironments,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.resource.DbStorageSingle
import com.digitalasset.canton.util.ResourceUtil

/** Verifies that all tables have a debug view that contains all columns. This is to keep the debug
  * views in sync with changes to the tables.
  */
trait ViewConsistencyTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with FlagCloseable
    with HasExecutionContext {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P0_S1M1

  private def tryCreateStorage(
      localInstance: LocalInstanceReference
  )(implicit env: TestConsoleEnvironment, closeContext: CloseContext): DbStorageSingle =
    DbStorageSingle.tryCreate(
      config = localInstance.config.storage.asInstanceOf[DbConfig],
      clock = env.environment.clock,
      scheduler = None,
      connectionPoolForParticipant = false,
      logQueryCost = None,
      metrics = CommonMockMetrics.dbStorage,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )

  "check that each table (and its columns) has a corresponding view" in { implicit env =>
    import env.*

    // when starting for the first time, migrations will run automatically
    sequencer1.start()

    implicit val closeContext = CloseContext(this)

    ResourceUtil.withResource(tryCreateStorage(sequencer1)) { storage =>
      import storage.api.*

      val inconsistentViews = storage
        .query(
          sql"""
            select
              table_name, column_name,
               case
                 when array_agg(table_schema::text) = array['public'] then 'public'
                 else 'debug'
               end
            from information_schema.columns
            where
              -- only compare tables in canton schemas
              table_schema in ('public', 'debug') and
              -- no views for flyway or blocks tables
              table_name not in ('flyway_schema_history', 'blocks') and
              -- DEV version adds a column, but it doesn't have any significance, so let's ignore it
              not (table_name = 'common_node_id' and column_name = 'test_column')
            group by table_name, column_name
            having count(column_name) != 2""".as[(String, String, String)],
          "select",
        )
        .futureValueUS

      val columnsOnlyInTables = inconsistentViews.collect { case (table, column, "public") =>
        s"$table.$column"
      }
      val columnsOnlyInViews = inconsistentViews.collect { case (table, column, "debug") =>
        s"$table.$column"
      }

      if (columnsOnlyInTables.nonEmpty || columnsOnlyInViews.nonEmpty)
        fail(
          s"""The following _table_ columns have a missing view counterpart:
           |${columnsOnlyInTables.mkString("\n")}
           |
           |The following _view_ columns have a missing table counterpart:
           |${columnsOnlyInViews.mkString("\n")}
           |Most likely the tables have changed, but the views haven't been updated accordingly.""".stripMargin
        )
      else succeed
    }

  }

}
class ViewConsistencyTestPostgres extends ViewConsistencyTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
