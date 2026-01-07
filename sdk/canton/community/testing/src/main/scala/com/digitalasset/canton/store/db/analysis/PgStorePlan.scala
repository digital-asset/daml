// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db.analysis

import slick.jdbc.{GetResult, PositionedResult}

import java.time.Instant

/** Data class representing a stored query plan from and pg_store_plans (all fields) and
  * pg_stat_statements (only query text).
  */
final case class PgStorePlan(
    queryId: Long,
    query: Option[String],
    planId: Long,
    userId: Long,
    dbId: Long,
    plan: String,
    calls: Long,
    totalTime: Double,
    rows: Long,
    firstCall: Instant,
    lastCall: Instant,
) {

  override def toString: String =
    s"""PgStorePlan:
       | ====================================================
       |  queryId:   $queryId
       |  query:
       | ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
       |  ${query.getOrElse("<not found>")}
       | ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
       |  planId:    $planId
       |  userId:    $userId
       |  dbId:      $dbId
       |  plan:
       | ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
       |  $plan
       | ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
       |  calls:     $calls
       |  totalTime: $totalTime
       |  rows:      $rows
       |  firstCall: $firstCall
       |  lastCall:  $lastCall
       | ====================================================
""".stripMargin

  lazy val parsedPlan: Either[io.circe.Error, PlanNode] =
    PlanWrapper.parseMaybeTruncatedJson(plan).map(_.plan)
}

object PgStorePlan {
  implicit val getPgStorePlanResult: GetResult[PgStorePlan] = GetResult { (r: PositionedResult) =>
    PgStorePlan(
      queryId = r.nextLong(), // queryid
      query = r.nextStringOption(), // query
      planId = r.nextLong(), // planid
      userId = r.nextLong(), // userid (OID mapped to Long)
      dbId = r.nextLong(), // dbid (OID mapped to Long)
      plan = r.nextString(), // plan
      calls = r.nextLong(), // calls
      totalTime = r.nextDouble(), // total_time
      rows = r.nextLong(), // rows
      firstCall = r.nextTimestamp().toInstant, // first_call
      lastCall = r.nextTimestamp().toInstant, // last_call
    )
  }
}
