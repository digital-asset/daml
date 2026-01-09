// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.analysis.PgStorePlan
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

import java.sql.SQLException

class PostgresStatsAnalysis(storage: DbStorage, override val timeouts: ProcessingTimeout)
    extends AsyncWordSpec
    with BaseTest
    with HasCloseContext
    with HasExecutionContext
    with FlagCloseable {

  import storage.api.*

  "Canton SQL queries" should {
    "not have expensive query plans" in {
      val plans = getAllPlansQuery.futureValueUS.flatMap(planRow =>
        planRow.parsedPlan match {
          case Left(error) =>
            logger.warn(s"Could not parse plan JSON for:\n$planRow")
            logger.warn(s"Parsing error: $error")
            Seq.empty
          case Right(_) =>
            Seq(planRow)
        }
      )
      plans should not be empty

      val expensivePlans = plans.view
        .filter(_.parsedPlan.value.totalCost > 1000.0)
        .toSeq
        .sortBy(_.parsedPlan.value.totalCost)(Ordering[Double].reverse)
      if (expensivePlans.nonEmpty) {
        logger.info(
          s"Found ${expensivePlans.size} expensive plans (cost > 1000.0):"
        )
        expensivePlans.foreach { plan =>
          logger.info(
            s"totalCost = ${plan.parsedPlan.value.totalCost}):\n$plan"
          )
        }
      }
      // TODO(#29772): Enable this assertion or develop a more sophisticated analysis of expensive plans.
      // expensivePlans should be(empty)
      succeed
    }
  }

  def getAllPlansQuery: FutureUnlessShutdown[Seq[PgStorePlan]] = {
    val query =
      sql"""
      SELECT
        plans.queryid,
        stats.query,
        plans.planid,
        plans.userid,
        plans.dbid,
        plans.plan,
        plans.calls,
        plans.total_time,
        plans.rows,
        plans.first_call,
        plans.last_call
      FROM pg_store_plans plans LEFT JOIN pg_stat_statements stats
        ON plans.queryid = stats.queryid
      WHERE plan NOT LIKE '%pg_%' -- we need to exclude Postgres internal queries from the report
    """.as[PgStorePlan]

    createExtensions.flatMap { _ =>
      storage.query(query, "get_all_pg_store_plans")
    }
  }

  private def createExtensions: FutureUnlessShutdown[Unit] = {
    val createExtensions = DBIO.seq(
      sqlu"CREATE EXTENSION IF NOT EXISTS pg_stat_statements",
      sqlu"CREATE EXTENSION IF NOT EXISTS pg_store_plans",
    )
    storage
      .update(createExtensions, "create_pg_stat_statements_and_store_plans")(
        TraceContext.empty,
        closeContext,
      )
      .recover {
        case e: SQLException if e.getSQLState == "42P07" => // duplicate
          Outcome(())
        case e: SQLException
            if e.getSQLState == "23505" && e.getMessage.contains(
              "duplicate key value violates unique constraint \"pg_extension_name_index\""
            ) =>
          Outcome(())
      }
  }
}
