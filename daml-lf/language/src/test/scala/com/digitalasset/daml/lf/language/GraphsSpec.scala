// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

import org.scalatest.Inside.inside
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.HashMap

class GraphsSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  import Graphs._

  "topoSort" should {

    val dags = Table[Graph[Int]](
      "directed acyclic graphs",
      HashMap.empty,
      HashMap(
        3 -> Set(8, 10),
        5 -> Set(11),
        7 -> Set(8, 11),
        8 -> Set(9),
        11 -> Set(2, 9, 10),
      ),
    )

    val dcgs = Table[Graph[String]](
      "directed cyclic graphs",
      HashMap("1" -> Set("1")),
      HashMap("A" -> Set("B"), "B" -> Set("A")),
      HashMap(
        "A" -> Set("B", "C", "E"),
        "B" -> Set("C", "E"),
        "C" -> Set("D"),
        "D" -> Set("B", "E"),
        "E" -> Set("E"),
      ),
    )

    "successfully sort all edges of directed acyclic graph" in {
      dags.forEvery { dag =>
        val result = topoSort(dag)
        result shouldBe a[Right[_, _]]

        inside(result) { case Right(sortedEdges) =>
          val allEdges = dag.values.foldLeft(dag.keySet)(_ | _)
          sortedEdges.toSet shouldBe allEdges

          val edgeRank = sortedEdges.zipWithIndex.toMap
          for {
            e <- dag.keys
            e_ <- dag(e)
          } edgeRank(e_) should be < edgeRank(e)
        }
      }
    }

    "fail on cyclic graph and return a proper cycle" in {
      dcgs.forEvery { dcg =>
        val result = topoSort(dcg)
        result shouldBe a[Left[_, _]]

        inside(result) { case Left(Cycle(loop)) =>
          ((loop.last :: loop) zip loop).foreach { case (e, e_) =>
            dcg(e) should contain(e_)
          }
        }
      }
    }
  }

}
