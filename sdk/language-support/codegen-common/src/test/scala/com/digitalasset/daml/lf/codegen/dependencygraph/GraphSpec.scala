// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.dependencygraph

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GraphSpec extends AnyFlatSpec with Matchers {

  private[this] def intNode(
      contentAndId: Int,
      deps: List[Int] = List.empty[Int],
  ): (Int, Node[Int, Int]) =
    contentAndId -> Node(contentAndId, deps)

  private[this] def orderedDependencies[K, A](
      nodes: Iterable[(K, Node[K, A])]
  ): OrderedDependencies[K, A] =
    Graph.cyclicDependencies(Iterable.empty, nodes)

  behavior of "Graph.cyclicDependencies"

  it should "accept a node with self-dependency" in {
    val node1 = intNode(1, List(1))
    orderedDependencies(Seq(node1)).deps should contain theSameElementsInOrderAs Seq(node1)
  }

  it should "return the empty Vector when the graph is empty" in {
    orderedDependencies(Seq()).deps shouldBe empty
  }

  it should "return the only element in the graph" in {
    val node = intNode(1)
    orderedDependencies(Seq(node)).deps should contain theSameElementsInOrderAs Seq(node)
  }

  it should "return the two independent elements of the graph" in {
    val node1 = intNode(1)
    val node2 = intNode(2)
    orderedDependencies(Seq(node1, node2)).deps should contain theSameElementsAs Seq(node1, node2)
  }

  it should "return the two elements ordered when the graph has one element depending from another one" in {
    val node1 = intNode(1, List(2))
    val node2 = intNode(2)
    orderedDependencies(Seq(node1, node2)).deps should contain theSameElementsInOrderAs Seq(
      node2,
      node1,
    )
  }

  it should "return the two elements connected ordered and the third one in any position" in {
    val node1 = intNode(1)
    val node2 = intNode(2)
    val node3 = intNode(3, List(1))
    val result = orderedDependencies(Seq(node1, node2, node3)).deps
    result should contain.inOrder(node1, node3)
    result should contain(node2)
  }

  it should "return the elements ordered when a complex graph is built" in {
    //      3 ~> 1
    // 2 ~> 3
    //      3 ~> 4
    // 2      ~> 4
    //           4 ~> 5
    val node1 = intNode(1)
    val node2 = intNode(2, List(3, 4))
    val node3 = intNode(3, List(4, 1))
    val node4 = intNode(4, List(5))
    val node5 = intNode(5)
    val result = orderedDependencies(Seq(node1, node2, node3, node4, node5)).deps
    result should contain(node5)
    result should contain.inOrder(node3, node2)
    result should contain.inOrder(node4, node2)
    result should contain.inOrder(node4, node3)
    result should contain.inOrder(node1, node3)
    result should contain.inOrder(node5, node4)
  }

  it should "return error for each unknown dependency" in {
    val node1 = intNode(1, List(2))
    val node2 = intNode(2)
    val node3 = intNode(3, List(6))
    val node4 = intNode(4, List(5, 6))
    val result = orderedDependencies(Seq(node1, node2, node3, node4))
    result.deps should contain theSameElementsInOrderAs Seq(node2, node1)
    result.errors should have length (2)
  }
}
