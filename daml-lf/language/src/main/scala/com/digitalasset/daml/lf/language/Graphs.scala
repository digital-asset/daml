// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

import com.daml.lf.data.{InsertOrdSet, Relation}

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

}
