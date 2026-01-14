// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.language

import com.digitalasset.daml.lf.data.{InsertOrdSet, Relation}
import scala.annotation.tailrec

object Graphs {

  final case class Cycle[X](vertices: List[X])

  type Graph[X] = Relation[X, X]

  // Topologically order the vertices of an abstract Graph.
  // If the `graph` is a directed acyclic graph returns a list of its vertices in topological order as `Right`
  // otherwise returns a cycle as `Left`.
  def topoSort[X](graph: Graph[X]): Either[Cycle[X], List[X]] = {

    var white = graph.keySet
    var black = graph.values.foldLeft(InsertOrdSet.empty[X])(_ | _.filterNot(white))
    def gray(x: X): Boolean = !white(x) && !black(x)

    def visitSet(xs: Set[X]): Option[X] = xs.foldLeft(Option.empty[X])(_ orElse visit(_))

    def visit(x: X): Option[X] =
      if (black(x))
        None
      else if (!white(x))
        Some(x)
      else { white -= x; visitSet(graph(x)) } orElse { black += x; None }

    def buildCycle(curr: X, start: X, list: List[X] = List.empty): Cycle[X] = {
      val next = graph(curr).find(gray).getOrElse(throw new UnknownError)
      if (next == start)
        Cycle(curr :: list)
      else
        buildCycle(next, start, curr :: list)
    }

    visitSet(graph.keySet).fold[Either[Cycle[X], List[X]]](Right(black.toList))(x =>
      Left(buildCycle(x, x))
    )
  }

  /*
   * Computes the transitive closure for a starting node in a graph.
   * The graph is represented as a map from a node to a set of its direct neighbors.
   *
   * @param graph The graph, represented as an adjacency map.
   * @param startNode The node from which to start the traversal.
   * @tparam A The type of the nodes in the graph.
   * @return A set containing all nodes reachable from the startNode (inclusive).
   */
  def transitiveClosure[A](graph: Graph[A], startNode: A): Set[A] = {

    @tailrec
    def go(toVisit: Set[A], visited: Set[A]): Set[A] = {
      if (toVisit.isEmpty) {
        visited
      } else {
        val current = toVisit.head
        val restToVisit = toVisit.tail

        if (visited.contains(current)) {
          go(restToVisit, visited)
        } else {
          val neighbors: Set[A] = Relation.lookup(graph, current)
          val newToVisit = restToVisit ++ neighbors
          val newVisited = visited + current
          go(newToVisit, newVisited)
        }
      }
    }
    go(Set(startNode), Set.empty[A])
  }

  /*
   * Computes the transitive closure of a directed graph.
   * The graph is represented as a map from a node to a set of its direct neighbors.
   *
   * @param g The graph, represented as an adjacency map.
   * @tparam A The type of the nodes in the graph.
   * @return A new graph where each node maps to the set of all nodes reachable from it.
   */
  def transitiveClosure[A](g: Graph[A]): Graph[A] = {
    val allVertices = g.keySet ++ g.values.flatten
    allVertices.view.map(u => u -> transitiveClosure(g, u)).toMap
  }
}
