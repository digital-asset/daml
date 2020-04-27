// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen.dependencygraph

import com.daml.codegen.exception.UnsopportedTypeError

object Graph {

  /**
    * Orders the nodes such that given a node n, its dependencies are placed in the resultant vector before n
    * This method checks for circular dependencies and will place the last-discovered
    * member of the cycle first.
    */
  def cyclicDependencies[K, A](
      internalNodes: TraversableOnce[(K, BaseNode[K, A])],
      roots: Iterable[(K, BaseNode[K, A])]): OrderedDependencies[K, A] = {
    type NKA = Node[K, A]
    type Seen = Map[K, Boolean]
    import collection.immutable.Set
    val graph = (roots ++ internalNodes).toMap

    def visit(
        seen: Seen,
        stack: Set[K],
        id: K,
        node: NKA): (Seen, Vector[(K, NKA)], Boolean, List[UnsopportedTypeError]) = {
      if (seen.isDefinedAt(id) || stack(id)) (seen, Vector(), seen getOrElse (id, false), List())
      else {
        val Node(_, deps, collectError) = node
        val (newSeen, newEnts, missing, utes) = visitN(seen, stack + id, deps)

        if (missing.nonEmpty)
          (
            newSeen,
            newEnts,
            true,
            UnsopportedTypeError(
              s"Type $id is not supported as dependencies have unsupported type: '${missing
                .mkString("', '")}'.") :: utes)
        else
          (newSeen, newEnts :+ ((id, node)), false, utes)
      }
    }

    def visitN(
        seen: Seen,
        stack: Set[K],
        deps: Iterable[K]): (Seen, Vector[(K, NKA)], List[K], List[UnsopportedTypeError]) =
      deps.foldLeft((seen, Vector[(K, NKA)](), List[K](), List[UnsopportedTypeError]())) {
        case ((seen, ents, missing, utes), k) =>
          graph get k collect {
            case n: NKA =>
              val (newSeen, newEnts, nMissing, newUtes) = visit(seen, stack, k, n)
              (
                newSeen updated (k, nMissing),
                ents ++ newEnts,
                if (nMissing) k :: missing else missing,
                newUtes ++ utes)
          } getOrElse {
            (seen updated (k, true), ents, k :: missing, utes)
          }
      }

    val (_, deps, _, utes) = visitN(Map(), Set(), roots map (_._1))
    OrderedDependencies(deps, utes)
  }
}

/**
  * Ordered dependencies where the dependant node always comes after the dependency.
  * @param deps The ordered dependencies
  * @param errors The errors that came up when producing the list
  */
final case class OrderedDependencies[+K, +A](
    deps: Vector[(K, Node[K, A])],
    errors: List[UnsopportedTypeError]
)
