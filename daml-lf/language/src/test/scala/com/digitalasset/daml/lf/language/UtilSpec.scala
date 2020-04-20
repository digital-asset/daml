package com.daml.lf.language

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

import scala.collection.immutable.HashMap

class UtilSpec extends WordSpec with Matchers with TableDrivenPropertyChecks {

  import Util._

  "topoSort" should {

    val dags = Table[Graph[Int]](
      "directed acyclic graphs",
      HashMap.empty,
      HashMap(
        3 -> Set(8, 10),
        5 -> Set(11),
        7 -> Set(8, 11),
        8 -> Set(9),
        11 -> Set(2, 9, 10)
      )
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
      )
    )

    "successfully sort edges of directed acyclic graph" in {
      dags.forEvery { dag =>
        val result = topoSort(dag)
        result shouldBe 'right

        val Right(sortedEdges) = result

        val allEdges = dag.values.foldLeft(dag.keySet)(_ | _)
        sortedEdges.toSet shouldBe allEdges

        val edgeRank = sortedEdges.zipWithIndex.toMap
        dag.foreach {
          case (e, dest) =>
            dest.foreach(e_ => edgeRank(e) > edgeRank(e_))
        }
      }
    }

    "fail on cyclic graph and return a cycle" in {
      dcgs.forEvery { dcg =>
        val result = topoSort(dcg)
        result shouldBe 'left

        val Left(loop) = result

        ((loop.last :: loop) zip loop).foreach {
          case (e, dest) =>
            dcg(e) should contain(dest)
        }
      }
    }
  }

}
