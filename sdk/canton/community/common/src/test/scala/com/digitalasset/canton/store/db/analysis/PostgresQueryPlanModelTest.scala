// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db.analysis

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class PostgresQueryPlanModelTest extends AnyWordSpec with BaseTest {
  "PlanWrapper" should {
    "correctly parse plan JSON with nested plans" in {
      val json =
        """
          |{
          |  "Plan": {
          |    "Node Type": "ModifyTable",
          |    "Operation": "Delete",
          |    "Parallel Aware": false,
          |    "Async Capable": false,
          |    "Relation Name": "par_dars",
          |    "Alias": "par_dars",
          |    "Startup Cost": 0.00,
          |    "Total Cost": 14.30,
          |    "Plan Rows": 0,
          |    "Plan Width": 0,
          |    "Plans": [
          |      {
          |        "Node Type": "Seq Scan",
          |        "Parent Relationship": "Outer",
          |        "Parallel Aware": false,
          |        "Async Capable": false,
          |        "Relation Name": "par_dars",
          |        "Alias": "par_dars",
          |        "Startup Cost": 0.00,
          |        "Total Cost": 14.30,
          |        "Plan Rows": 430,
          |        "Plan Width": 6
          |      }
          |    ]
          |  }
          |}
          |""".stripMargin

      val plan = PlanWrapper.parseMaybeTruncatedJson(json).valueOrFail("parsing failed")
      plan.plan.nodeType shouldBe "ModifyTable"
      plan.plan.relationName shouldBe Some("par_dars")
      plan.plan.startupCost shouldBe 0.0
      plan.plan.totalCost shouldBe 14.3
      plan.plan.planRows shouldBe 0.0
      plan.plan.planWidth shouldBe 0
      plan.plan.other.get("Operation").flatMap(_.asString) should contain("Delete")
      plan.plan.other.get("Alias").flatMap(_.asString) should contain("par_dars")
      plan.plan.plans should have size 1
      plan.plan.plans.headOption.value.nodeType shouldBe "Seq Scan"

      val json2 =
        """
          |{
          |  "Plan": {
          |    "Node Type": "Seq Scan",
          |    "Parallel Aware": false,
          |    "Async Capable": false,
          |    "Relation Name": "sequencer_members",
          |    "Alias": "sequencer_members",
          |    "Startup Cost": 29.90,
          |    "Total Cost": 5455250.60,
          |    "Plan Rows": 167,
          |    "Plan Width": 40,
          |    "Filter": "(enabled AND (registered_ts <= '1764856154976253'::bigint))",
          |    "Plans": [
          |      {
          |        "Node Type": "Seq Scan",
          |        "Parent Relationship": "InitPlan",
          |        "Subplan Name": "CTE watermarks",
          |        "Parallel Aware": false,
          |        "Async Capable": false,
          |        "Relation Name": "sequencer_watermarks",
          |        "Alias": "sequencer_watermarks",
          |        "Startup Cost": 0.00,
          |        "Total Cost": 29.90,
          |        "Plan Rows": 1990,
          |        "Plan Width": 12
          |      },
          |      {
          |        "Node Type": "Limit",
          |        "Parent Relationship": "SubPlan",
          |        "Subplan Name": "SubPlan 3",
          |        "Parallel Aware": false,
          |        "Async Capable": false,
          |        "Startup Cost": 16332.92,
          |        "Total Cost": 16332.93,
          |        "Plan Rows": 1,
          |        "Plan Width": 8,
          |        "Plans": [
          |          {
          |            "Node Type": "Sort",
          |            "Parent Relationship": "Outer",
          |            "Parallel Aware": false,
          |            "Async Capable": false,
          |            "Startup Cost": 16332.92,
          |            "Total Cost": 16337.90,
          |            "Plan Rows": 1990,
          |            "Plan Width": 8,
          |            "Sort Key": ["((SubPlan 2)) DESC"],
          |            "Plans": [
          |              {
          |                "Node Type": "CTE Scan",
          |                "Parent Relationship": "Outer",
          |                "Parallel Aware": false,
          |                "Async Capable": false,
          |                "CTE Name": "watermarks",
          |                "Alias": "watermarks",
          |                "Startup Cost": 0.00,
          |                "Total Cost": 16322.97,
          |                "Plan Rows": 1990,
          |                "Plan Width": 8,
          |                "Plans": [
          |                  {
          |                    "Node Type": "Limit",
          |                    "Parent Relationship": "SubPlan",
          |                    "Subplan Name": "SubPlan 2",
          |                    "Parallel Aware": false,
          |                    "Async Capable": false,
          |                    "Startup Cost": 0.15,
          |                    "Total Cost": 8.18,
          |                    "Plan Rows": 1,
          |                    "Plan Width": 14,
          |                    "Plans": [
          |                      {
          |                        "Node Type": "Index Only Scan",
          |                        "Parent Relationship": "Outer",
          |                        "Parallel Aware": false,
          |                        "Async Capable": false,
          |                        "Scan Direction": "Backward",
          |                        "Index Name": "sequencer_event_recipients_pkey",
          |                        "Relation Name": "sequencer_event_recipients",
          |                        "Alias": "member_recipient",
          |                        "Startup Cost": 0.15,
          |                        "Total Cost": 8.18,
          |                        "Plan Rows": 1,
          |                        "Plan Width": 14,
          |                        "Index Cond": "((node_index = watermarks.node_index) AND (recipient_id = '-1'::integer) AND (ts <= watermarks.watermark_ts) AND (ts <= '1764856154976253'::bigint) AND (ts <= '1764856154976253'::bigint) AND (ts > sequencer_members.registered_ts))"
          |                      }
          |                    ]
          |                  }
          |                ]
          |              }
          |            ]
          |          }
          |        ]
          |      },
          |      {
          |        "Node Type": "Limit",
          |        "Parent Relationship": "SubPlan",
          |        "Subplan Name": "SubPlan 5",
          |        "Parallel Aware": false,
          |        "Async Capable": false,
          |        "Startup Cost": 16332.92,
          |        "Total Cost": 16332.93,
          |        "Plan Rows": 1,
          |        "Plan Width": 8,
          |        "Plans": [
          |          {
          |            "Node Type": "Sort",
          |            "Parent Relationship": "Outer",
          |            "Parallel Aware": false,
          |            "Async Capable": false,
          |            "Startup Cost": 16332.92,
          |            "Total Cost": 16337.90,
          |            "Plan Rows": 1990,
          |            "Plan Width": 8,
          |            "Sort Key": ["((SubPlan 4)) DESC"],
          |            "Plans": [
          |              {
          |                "Node Type": "CTE Scan",
          |                "Parent Relationship": "Outer",
          |                "Parallel Aware": false,
          |                "Async Capable": false,
          |                "CTE Name": "watermarks",
          |                "Alias": "watermarks_1",
          |                "Startup Cost": 0.00,
          |                "Total Cost": 16322.97,
          |                "Plan Rows": 1990,
          |                "Plan Width": 8,
          |                "Plans": [
          |                  {
          |                    "Node Type": "Limit",
          |                    "Parent Relationship": "SubPlan",
          |                    "Subplan Name": "SubPlan 4",
          |                    "Parallel Aware": false,
          |                    "Async Capable": false,
          |                    "Startup Cost": 0.15,
          |                    "Total Cost": 8.18,
          |                    "Plan Rows": 1,
          |                    "Plan Width": 14,
          |                    "Plans": [
          |                      {
          |                        "Node Type": "Index Only Scan",
          |                        "Parent Relationship": "Outer",
          |                        "Parallel Aware": false,
          |                        "Async Capable": false,
          |                        "Scan Direction": "Backward",
          |                        "Index Name": "sequencer_event_recipients_pkey",
          |                        "Relation Name": "sequencer_event_recipients",
          |                        "Alias": "member_recipient_1",
          |                        "Startup Cost": 0.15,
          |                        "Total Cost": 8.18,
          |                        "Plan Rows": 1,
          |                        "Plan Width": 14,
          |                        "Index Cond": "((node_index = watermarks_1.node_index) AND (recipient_id = sequencer_members.id) AND (ts <= watermarks_1.watermark_ts) AND (ts <= '1764856154976253'::bigint) AND (ts <= '1764856154976253'::bigint) AND (ts > sequencer_members.registered_ts))"
          |                      }
          |                    ]
          |                  }
          |                ]
          |              }
          |            ]
          |          }
          |        ]
          |      }
          |    ]
          |  }
          |}
          |""".stripMargin

      val plan2 = PlanWrapper.parseMaybeTruncatedJson(json2).valueOrFail("parsing failed 2")
      plan2.plan.nodeType shouldBe "Seq Scan"
      plan2.plan.plans should have size 3

      val json3 =
        """
          |{
          |  "Plan": {
          |    "Node Type": "Gather Merge",
          |    "Parallel Aware": false,
          |    "Async Capable": false,
          |    "Startup Cost": 14547.54,
          |    "Total Cost": 28625.52,
          |    "Plan Rows": 120660,
          |    "Plan Width": 102,
          |    "Workers Planned": 2,
          |    "Plans": [
          |      {
          |        "Node Type": "Sort",
          |        "Parent Relationship": "Outer",
          |        "Parallel Aware": false,
          |        "Async Capable": false,
          |        "Startup Cost": 13547.52,
          |        "Total Cost": 13698.34,
          |        "Plan Rows": 60330,
          |        "Plan Width": 102,
          |        "Sort Key": ["ts", "repair_counter"],
          |        "Plans": [
          |          {
          |            "Node Type": "Seq Scan",
          |            "Parent Relationship": "Outer",
          |            "Parallel Aware": true,
          |            "Async Capable": false,
          |            "Relation Name": "par_active_contracts",
          |            "Alias": "par_active_contracts",
          |            "Startup Cost": 250.00,
          |            "Total Cost": 5456.64,
          |            "Plan Rows": 60330,
          |            "Plan Width": 102
          |<truncated>
          |""".stripMargin

      val plan3 = PlanWrapper.parseMaybeTruncatedJson(json3).valueOrFail("parsing failed 3")
      plan3.plan.nodeType shouldBe "Gather Merge"
      plan3.plan.totalCost shouldBe 28625.52

      val json4 =
        """
          |{
          |  "Plan": {
          |    "Node Type": "Bitmap Heap Scan",
          |    "Parallel Aware": false,
          |    "Async Capable": false,
          |    "Relation Name": "par_contracts",
          |    "Alias": "par_contracts",
          |    "Startup Cost": 13.57,
          |    "Total Cost": 54.10,
          |    "Plan Rows": 71,
          |    "Plan Width": 40
          |<truncated>
          |""".stripMargin
      val plan4 = PlanWrapper.parseMaybeTruncatedJson(json4).valueOrFail("parsing failed 4")
      plan4.plan.nodeType shouldBe "Bitmap Heap Scan"
      plan4.plan.totalCost shouldBe 54.10
    }
  }

}
