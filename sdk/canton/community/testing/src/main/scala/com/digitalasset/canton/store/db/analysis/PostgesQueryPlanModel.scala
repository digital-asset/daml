// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db.analysis

import io.circe.*
import io.circe.parser.decode

/** Represents a single node in the Postgres Query Plan tree. Updated to capture Rows, Width, and a
  * catch-all map for other fields.
  */
final case class PlanNode(
    nodeType: String,
    relationName: Option[String],
    startupCost: Double,
    totalCost: Double,
    planRows: Double, // "Plan Rows" - using Double as estimates can be fractional
    planWidth: Int, // "Plan Width"
    other: Map[String, Json], // Captures "Output", "Filter", "Alias", etc.
    plans: List[PlanNode],
)

object PlanNode {

  // Keys that we explicitly map to struct fields.
  // Everything else goes into 'other'.
  private val ExplicitKeys = Set(
    "Node Type",
    "Relation Name",
    "Startup Cost",
    "Total Cost",
    "Plan Rows",
    "Plan Width",
    "Plans",
  )

  implicit lazy val listDecoder: Decoder[List[PlanNode]] = Decoder.decodeList[PlanNode]
  implicit val decoder: Decoder[PlanNode] = Decoder.instance { c =>
    for {
      // 1. Decode the raw JsonObject to extract dynamic "other" fields later
      jsonObj <- c.as[JsonObject]

      // 2. Decode explicit fields
      nodeType <- c.downField("Node Type").as[String]
      relationName <- c.downField("Relation Name").as[Option[String]]
      startupCost <- c.downField("Startup Cost").as[Double]
      totalCost <- c.downField("Total Cost").as[Double]
      planRows <- c.downField("Plan Rows").as[Double]
      planWidth <- c.downField("Plan Width").as[Int]

      // 3. Recursive decode for children. Default to Nil if "Plans" is missing (leaf nodes)
      plans <- c.downField("Plans").as[List[PlanNode]].orElse(Right(Nil))
    } yield {
      // 4. Filter the raw object to keep only keys we haven't handled explicitly
      val otherMap = jsonObj.filterKeys(key => !ExplicitKeys.contains(key)).toMap

      PlanNode(
        nodeType,
        relationName,
        startupCost,
        totalCost,
        planRows,
        planWidth,
        otherMap,
        plans,
      )
    }
  }
}

/** Wrapper for the top-level Postgres JSON structure
  */
final case class PlanWrapper(plan: PlanNode)

object PlanWrapper {
  implicit val decoder: Decoder[PlanWrapper] = Decoder.instance { c =>
    c.downField("Plan").as[PlanNode].map(PlanWrapper(_))
  }

  /** Query plan may be truncated in the middle of the JSON structure, but still contains usable
    * top-level fields. This method attempts to fix such truncated JSON strings before parsing.
    */
  def parseMaybeTruncatedJson(jsonStr: String): Either[io.circe.Error, PlanWrapper] = {
    val isTruncated = jsonStr.contains("<truncated>")
    val hasPlans = jsonStr.contains("\"Plans\"")
    val fixedJsonPlan = if (isTruncated) {
      if (hasPlans) {
        jsonStr.replaceAll("(?s),\\s*\"Plans\".*<truncated>.*", "\n  }\n}")
      } else {
        jsonStr.replaceAll("<truncated>", "\n  }\n}")
      }
    } else {
      jsonStr
    }
    decode[PlanWrapper](fixedJsonPlan)
  }
}
