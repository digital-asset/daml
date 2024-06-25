// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.dependencygraph

import com.digitalasset.daml.lf.codegen.exception.UnsupportedTypeError

object Graph {

  /** Orders the nodes such that given a node n, its dependencies are placed in the resultant vector before n
    * This method checks for circular dependencies and will place the last-discovered
    * member of the cycle first.
    */
  def cyclicDependencies[K, A](
      internalNodes: IterableOnce[(K, Node[K, A])],
      roots: Iterable[(K, Node[K, A])],
  ): OrderedDependencies[K, A] = {
    type NKA = Node[K, A]
    type Seen = Map[K, Boolean]
    import collection.immutable.Set
    val graph = (roots ++ internalNodes).toMap

    def visit(
        seen: Seen,
        stack: Set[K],
        id: K,
        node: NKA,
    ): (Seen, Vector[(K, NKA)], Boolean, List[UnsupportedTypeError]) = {
      if (seen.isDefinedAt(id) || stack(id)) {
        (seen, Vector(), seen.getOrElse(id, false), List())
      } else {
        visitN(seen, stack + id, node.dependencies) match {
          case (newSeen, newEnts, Nil, utes) =>
            (newSeen, newEnts :+ ((id, node)), false, utes)
          case (newSeen, newEnts, missing, utes) =>
            (newSeen, newEnts, true, UnsupportedTypeError(id, missing) :: utes)
        }
      }
    }

    def visitN(
        seen: Seen,
        stack: Set[K],
        deps: Iterable[K],
    ): (Seen, Vector[(K, NKA)], List[K], List[UnsupportedTypeError]) =
      deps.foldLeft((seen, Vector[(K, NKA)](), List[K](), List[UnsupportedTypeError]())) {
        case ((seen, ents, missing, utes), k) =>
          graph get k collect { case n: NKA =>
            val (newSeen, newEnts, nMissing, newUtes) = visit(seen, stack, k, n)
            (
              newSeen.updated(k, nMissing),
              ents ++ newEnts,
              if (nMissing) k :: missing else missing,
              newUtes ++ utes,
            )
          } getOrElse {
            (seen.updated(k, true), ents, k :: missing, utes)
          }
      }

    val (_, deps, _, utes) = visitN(Map(), Set(), roots map (_._1))
    OrderedDependencies(deps, utes)
  }
}
